import os
from pathlib import Path
from typing import Any, Optional, Tuple

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from databricks import sql

st.set_page_config(page_title="Smart Maintenance Dashboard", page_icon="🛠️", layout="wide")

DEFAULT_CATALOG = "workspace"
DEFAULT_SCHEMA = "smart_maintenance"
FEATURES_TABLE_NAME = "gold_ai4i2020_features"
SILVER_TABLE_NAME = "silver_ai4i2020_validated"
PLACEHOLDER_PREFIXES = (
    "SEU_",
    "adb-1234567890123456",
    "/sql/1.0/warehouses/xxxxxxxxxxxxxxxx",
    "dapiXXXXXXXXXXXXXXXX",
)


def _get_secret(name: str, default: Optional[str] = None) -> Optional[str]:
    if name in st.secrets:
        return st.secrets[name]
    return os.getenv(name, default)


def _get_bool(name: str, default: bool = False) -> bool:
    raw_value = _get_secret(name, str(default).lower())
    return str(raw_value).strip().lower() in {"1", "true", "yes", "y", "on"}


def get_connection() -> Any:
    hostname = _get_secret("DATABRICKS_SERVER_HOSTNAME")
    http_path = _get_secret("DATABRICKS_HTTP_PATH")
    access_token = _get_secret("DATABRICKS_ACCESS_TOKEN")

    hostname = str(hostname).strip() if hostname else hostname
    http_path = str(http_path).strip() if http_path else http_path
    access_token = str(access_token).strip() if access_token else access_token

    if hostname and hostname.startswith("https://"):
        hostname = hostname.replace("https://", "", 1)
    if hostname and hostname.startswith("http://"):
        hostname = hostname.replace("http://", "", 1)

    if http_path and not http_path.startswith("/"):
        http_path = f"/{http_path}"

    if access_token and access_token.lower().startswith("bearer "):
        access_token = access_token.split(" ", 1)[1].strip()

    # Correção de erro comum de cópia: token iniciando com "Sdapi".
    if access_token and access_token.startswith("Sdapi"):
        access_token = access_token[1:]

    if not hostname or not http_path or not access_token:
        raise RuntimeError(
            "Configure DATABRICKS_SERVER_HOSTNAME, DATABRICKS_HTTP_PATH e DATABRICKS_ACCESS_TOKEN em .streamlit/secrets.toml ou variáveis de ambiente."
        )

    if (
        any(str(hostname).startswith(prefix) for prefix in PLACEHOLDER_PREFIXES)
        or any(str(http_path).startswith(prefix) for prefix in PLACEHOLDER_PREFIXES)
        or any(str(access_token).startswith(prefix) for prefix in PLACEHOLDER_PREFIXES)
    ):
        raise RuntimeError(
            "Os secrets do Databricks ainda estão com valores de exemplo. Atualize .streamlit/secrets.toml com os valores reais de DATABRICKS_SERVER_HOSTNAME, DATABRICKS_HTTP_PATH e DATABRICKS_ACCESS_TOKEN."
        )

    try:
        return sql.connect(
            server_hostname=hostname,
            http_path=http_path,
            access_token=access_token,
        )
    except Exception as exc:
        raise RuntimeError(
            "Falha de autenticação no Databricks SQL Warehouse. Verifique: "
            "(1) token PAT válido e ativo (prefixo normalmente 'dapi'); "
            "(2) hostname sem https://; "
            "(3) HTTP Path do SQL Warehouse iniciando com /sql/1.0/warehouses/. "
            f"Detalhe: {exc}"
        ) from exc


@st.cache_data(ttl=300)
def run_query(query: str, params: Optional[Tuple] = None) -> pd.DataFrame:
    with get_connection() as connection:
        with connection.cursor() as cursor:
            if params is None:
                cursor.execute(query)
            else:
                cursor.execute(query, parameters=params)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
    return pd.DataFrame(rows, columns=columns)


@st.cache_data(ttl=300)
def resolve_table_by_name(table_name: str, configured_catalog: str, configured_schema: str) -> Optional[str]:
    direct_table = f"{configured_catalog}.{configured_schema}.{table_name}"
    checks = [
        f"SELECT 1 FROM {direct_table} LIMIT 1",
        f"SELECT 1 FROM {configured_schema}.{table_name} LIMIT 1",
        f"SELECT 1 FROM {table_name} LIMIT 1",
    ]

    for idx, check_query in enumerate(checks):
        try:
            run_query(check_query)
            if idx == 0:
                return direct_table
            if idx == 1:
                return f"{configured_schema}.{table_name}"
            return table_name
        except Exception:
            continue

    show_checks = [
        f"SHOW TABLES IN {configured_catalog}.{configured_schema} LIKE '{table_name}'",
        f"SHOW TABLES IN {configured_schema} LIKE '{table_name}'",
        f"SHOW TABLES LIKE '{table_name}'",
    ]
    for idx, show_query in enumerate(show_checks):
        try:
            shown = run_query(show_query)
            if not shown.empty:
                if idx == 0:
                    return direct_table
                if idx == 1:
                    return f"{configured_schema}.{table_name}"
                return table_name
        except Exception:
            continue

    discovery_query = f"""
    SELECT CONCAT(table_catalog, '.', table_schema, '.', table_name) AS fq_table
    FROM system.information_schema.tables
    WHERE lower(table_name) = lower('{table_name}')
    ORDER BY
      CASE WHEN lower(table_catalog) = lower('{configured_catalog}') THEN 0 ELSE 1 END,
      CASE WHEN lower(table_schema) = lower('{configured_schema}') THEN 0 ELSE 1 END,
      table_catalog,
      table_schema
    LIMIT 1
    """

    try:
        discovered = run_query(discovery_query)
        if not discovered.empty:
            return str(discovered.iloc[0]["fq_table"])
    except Exception:
        pass

    return None


def resolve_failure_columns(features_table: str) -> dict[str, str]:
    try:
        sample = run_query(f"SELECT * FROM {features_table} LIMIT 1")
        columns = {str(col).lower() for col in sample.columns}
    except Exception:
        columns = set()

    if {"twf_label", "hdf_label", "pwf_label", "osf_label", "rnf_label"}.issubset(columns):
        return {
            "twf": "twf_label",
            "hdf": "hdf_label",
            "pwf": "pwf_label",
            "osf": "osf_label",
            "rnf": "rnf_label",
        }

    return {
        "twf": "twf",
        "hdf": "hdf",
        "pwf": "pwf",
        "osf": "osf",
        "rnf": "rnf",
    }


@st.cache_data(ttl=300)
def resolve_features_source(
    configured_catalog: str,
    configured_schema: str,
    explicit_features_table: Optional[str] = None,
) -> tuple[str, dict[str, str], str]:
    if explicit_features_table:
        explicit_features_table = explicit_features_table.strip()
        run_query(f"SELECT 1 FROM {explicit_features_table} LIMIT 1")
        return (
            explicit_features_table,
            resolve_failure_columns(explicit_features_table),
            "Explicit",
        )

    gold_table = resolve_table_by_name(FEATURES_TABLE_NAME, configured_catalog, configured_schema)
    if gold_table is not None:
        return (
            gold_table,
            resolve_failure_columns(gold_table),
            "Gold",
        )

    silver_table = resolve_table_by_name(SILVER_TABLE_NAME, configured_catalog, configured_schema)
    if silver_table is not None:
        return (
            silver_table,
            resolve_failure_columns(silver_table),
            "Silver",
        )

    raise RuntimeError(
        "Tabela de origem não encontrada. Defina AI4I_TARGET_CATALOG/AI4I_TARGET_SCHEMA com o local correto "
        f"ou crie {configured_catalog}.{configured_schema}.{FEATURES_TABLE_NAME} (preferencial) "
        f"ou {configured_catalog}.{configured_schema}.{SILVER_TABLE_NAME}."
    )


@st.cache_data(ttl=300)
def get_filter_options(features_table: str) -> tuple[list[str], list[str]]:
    query = f"""
    SELECT DISTINCT product_id, machine_type
    FROM {features_table}
    ORDER BY product_id
    """
    df = run_query(query)
    product_ids = [value for value in df["product_id"].dropna().astype(str).unique().tolist()]
    machine_types = sorted([value for value in df["machine_type"].dropna().astype(str).unique().tolist()])
    return product_ids, machine_types


def build_filter_clause(selected_product_id: str, selected_machine_type: str) -> tuple[str, list[str]]:
    conditions = []
    params: list[str] = []

    if selected_product_id != "Todos":
        conditions.append("product_id = ?")
        params.append(selected_product_id)

    if selected_machine_type != "Todos":
        conditions.append("machine_type = ?")
        params.append(selected_machine_type)

    where_clause = " AND ".join(conditions)
    if where_clause:
        where_clause = "WHERE " + where_clause
    return where_clause, params


def render_gauge(risk_score: float, avg_tool_wear: float) -> None:
    fig = go.Figure(
        go.Indicator(
            mode="gauge+number",
            value=risk_score,
            number={"suffix": "%"},
            title={"text": f"Risco de Falha por Desgaste<br><sup>Tool wear médio: {avg_tool_wear:.1f} min</sup>"},
            gauge={
                "axis": {"range": [0, 100]},
                "bar": {"color": "#ef4444"},
                "steps": [
                    {"range": [0, 35], "color": "#10b981"},
                    {"range": [35, 70], "color": "#f59e0b"},
                    {"range": [70, 100], "color": "#ef4444"},
                ],
            },
        )
    )
    fig.update_layout(height=320, margin=dict(l=20, r=20, t=60, b=20))
    st.plotly_chart(fig, use_container_width=True)


def load_local_features_dataframe() -> pd.DataFrame:
    local_file = Path(__file__).resolve().parents[1] / "ai4i2020.xls"
    if not local_file.exists():
        raise RuntimeError(
            "Não foi possível localizar tabelas no Databricks e o arquivo local ai4i2020.xls também não foi encontrado."
        )

    df: Optional[pd.DataFrame] = None
    load_errors: list[str] = []

    for excel_engine in ("openpyxl", "xlrd"):
        try:
            df = pd.read_excel(local_file, engine=excel_engine)
            break
        except Exception as exc:
            load_errors.append(f"Excel engine '{excel_engine}': {exc}")

    if df is None:
        try:
            df = pd.read_csv(local_file, sep=None, engine="python")
        except Exception as exc:
            load_errors.append(f"CSV fallback: {exc}")

    if df is None:
        details = " | ".join(load_errors)
        raise RuntimeError(
            "Falha ao ler o arquivo local ai4i2020.xls. "
            "Verifique se o arquivo é um Excel válido (ou CSV renomeado) e se as dependências estão instaladas. "
            f"Detalhes: {details}"
        )

    rename_map = {
        "Product ID": "product_id",
        "Type": "machine_type",
        "Air temperature [K]": "air_temperature_k",
        "Process temperature [K]": "process_temperature_k",
        "Rotational speed [rpm]": "rotational_speed_rpm",
        "Torque [Nm]": "torque_nm",
        "Tool wear [min]": "tool_wear_min",
        "TWF": "twf_label",
        "HDF": "hdf_label",
        "PWF": "pwf_label",
        "OSF": "osf_label",
        "RNF": "rnf_label",
    }
    df = df.rename(columns=rename_map)

    required_columns = {
        "product_id",
        "machine_type",
        "air_temperature_k",
        "torque_nm",
        "tool_wear_min",
        "twf_label",
        "hdf_label",
        "pwf_label",
        "osf_label",
        "rnf_label",
    }
    missing = required_columns.difference(df.columns)
    if missing:
        raise RuntimeError(
            "Arquivo local carregado, mas faltam colunas esperadas para o dashboard: "
            + ", ".join(sorted(missing))
        )

    return df


def apply_local_filters(df: pd.DataFrame, product_id: str, machine_type: str) -> pd.DataFrame:
    filtered = df.copy()
    if product_id != "Todos":
        filtered = filtered[filtered["product_id"].astype(str) == str(product_id)]
    if machine_type != "Todos":
        filtered = filtered[filtered["machine_type"].astype(str) == str(machine_type)]
    return filtered


def main() -> None:
    st.title("Smart Maintenance Lakehouse Dashboard")
    st.caption("Streamlit conectado diretamente ao SQL Warehouse do Databricks via databricks-sql-python.")

    catalog = _get_secret("AI4I_TARGET_CATALOG", DEFAULT_CATALOG)
    schema = _get_secret("AI4I_TARGET_SCHEMA", DEFAULT_SCHEMA)
    explicit_features_table = _get_secret("AI4I_FEATURES_TABLE")
    allow_local_fallback = _get_bool("AI4I_ALLOW_LOCAL_FALLBACK", True)
    use_databricks_source = True
    local_df: Optional[pd.DataFrame] = None

    try:
        features_table, failure_columns, source_layer = resolve_features_source(
            catalog,
            schema,
            explicit_features_table,
        )
        product_ids, machine_types = get_filter_options(features_table)
    except Exception as exc:
        message = str(exc)
        if "UC_HIVE_METASTORE_DISABLED_EXCEPTION" in message and str(catalog).lower() == "hive_metastore":
            catalog = "main"
            st.warning(
                "Hive Metastore está desabilitado neste workspace. Alternando automaticamente para Unity Catalog (`main`)."
            )
            try:
                features_table, failure_columns, source_layer = resolve_features_source(
                    catalog,
                    schema,
                    explicit_features_table,
                )
                product_ids, machine_types = get_filter_options(features_table)
            except Exception as retry_exc:
                st.error(
                    "Falha ao consultar no catálogo `main`. Verifique se a tabela existe em `main.smart_maintenance` "
                    "ou ajuste `AI4I_TARGET_CATALOG` e `AI4I_TARGET_SCHEMA` no secrets.\n\n"
                    f"Detalhe: {retry_exc}"
                )
                st.stop()
        else:
            if allow_local_fallback:
                st.warning(
                    "Fonte Databricks não encontrada. Usando fallback local via arquivo ai4i2020.xls para visualização."
                )
                use_databricks_source = False
                local_df = load_local_features_dataframe()
                features_table = "ai4i2020.xls"
                source_layer = "LocalFile"
                failure_columns = {
                    "twf": "twf_label",
                    "hdf": "hdf_label",
                    "pwf": "pwf_label",
                    "osf": "osf_label",
                    "rnf": "rnf_label",
                }
                product_ids = sorted(local_df["product_id"].dropna().astype(str).unique().tolist())
                machine_types = sorted(local_df["machine_type"].dropna().astype(str).unique().tolist())
            else:
                st.error(
                    "Fonte Databricks não encontrada e fallback local está desabilitado. "
                    "Ajuste AI4I_TARGET_CATALOG/AI4I_TARGET_SCHEMA, ou defina AI4I_FEATURES_TABLE com o nome completo da tabela."
                )
                st.info(
                    "Exemplo: AI4I_FEATURES_TABLE = 'main.smart_maintenance.gold_ai4i2020_features'"
                )
                st.stop()

    with st.sidebar:
        st.header("Filtros Dinâmicos")
        selected_product_id = st.selectbox("Produto", ["Todos"] + product_ids)
        selected_machine_type = st.selectbox("Tipos", ["Todos"] + machine_types)
        if use_databricks_source:
            st.success("Fonte: Databricks SQL Warehouse")
        else:
            st.warning("Fonte: Fallback local (ai4i2020.xls)")

    if use_databricks_source:
        where_clause, params = build_filter_clause(selected_product_id, selected_machine_type)

        health_query = f"""
        SELECT
            COALESCE(AVG(tool_wear_min), 0) AS avg_tool_wear,
            COALESCE(MAX(tool_wear_min), 0) AS max_tool_wear,
            COUNT(*) AS total_records
        FROM {features_table}
        {where_clause}
        """
        health_df = run_query(health_query, tuple(params))
        avg_tool_wear = float(health_df.iloc[0]["avg_tool_wear"]) if not health_df.empty else 0.0
        max_tool_wear = float(health_df.iloc[0]["max_tool_wear"]) if not health_df.empty else 0.0

        corr_query = f"""
        SELECT
            ROUND(air_temperature_k, 1) AS air_temperature_k,
            ROUND(torque_nm, 1) AS torque_nm,
            COUNT(*) AS frequency
        FROM {features_table}
        {where_clause}
        GROUP BY ROUND(air_temperature_k, 1), ROUND(torque_nm, 1)
        ORDER BY air_temperature_k, torque_nm
        """
        corr_df = run_query(corr_query, tuple(params))

        failure_query = f"""
        SELECT 'TWF' AS failure_type, COALESCE(SUM({failure_columns['twf']}), 0) AS total_failures
        FROM {features_table}
        {where_clause}
        UNION ALL
        SELECT 'HDF' AS failure_type, COALESCE(SUM({failure_columns['hdf']}), 0) AS total_failures
        FROM {features_table}
        {where_clause}
        UNION ALL
        SELECT 'PWF' AS failure_type, COALESCE(SUM({failure_columns['pwf']}), 0) AS total_failures
        FROM {features_table}
        {where_clause}
        UNION ALL
        SELECT 'OSF' AS failure_type, COALESCE(SUM({failure_columns['osf']}), 0) AS total_failures
        FROM {features_table}
        {where_clause}
        UNION ALL
        SELECT 'RNF' AS failure_type, COALESCE(SUM({failure_columns['rnf']}), 0) AS total_failures
        FROM {features_table}
        {where_clause}
        """
        failure_params = tuple(params + params + params + params + params)
        failure_df = run_query(failure_query, failure_params)

        summary_query = f"""
        SELECT
            COUNT(*) AS total_records,
            COUNT(DISTINCT product_id) AS total_products,
            COUNT(DISTINCT machine_type) AS total_types
        FROM {features_table}
        {where_clause}
        """
        summary_df = run_query(summary_query, tuple(params))
        total_records = int(summary_df.iloc[0]["total_records"]) if not summary_df.empty else 0
        total_products = int(summary_df.iloc[0]["total_products"]) if not summary_df.empty else 0
        total_types = int(summary_df.iloc[0]["total_types"]) if not summary_df.empty else 0
    else:
        assert local_df is not None
        filtered_df = apply_local_filters(local_df, selected_product_id, selected_machine_type)
        avg_tool_wear = float(filtered_df["tool_wear_min"].mean()) if not filtered_df.empty else 0.0
        max_tool_wear = float(filtered_df["tool_wear_min"].max()) if not filtered_df.empty else 0.0
        corr_df = (
            filtered_df.assign(
                air_temperature_k=filtered_df["air_temperature_k"].round(1),
                torque_nm=filtered_df["torque_nm"].round(1),
            )
            .groupby(["air_temperature_k", "torque_nm"], as_index=False)
            .size()
            .rename(columns={"size": "frequency"})
            .sort_values(["air_temperature_k", "torque_nm"])
        )
        failure_df = pd.DataFrame(
            {
                "failure_type": ["TWF", "HDF", "PWF", "OSF", "RNF"],
                "total_failures": [
                    int(filtered_df[failure_columns["twf"]].sum()),
                    int(filtered_df[failure_columns["hdf"]].sum()),
                    int(filtered_df[failure_columns["pwf"]].sum()),
                    int(filtered_df[failure_columns["osf"]].sum()),
                    int(filtered_df[failure_columns["rnf"]].sum()),
                ],
            }
        )
        total_records = int(len(filtered_df))
        total_products = int(filtered_df["product_id"].nunique()) if not filtered_df.empty else 0
        total_types = int(filtered_df["machine_type"].nunique()) if not filtered_df.empty else 0

    denominator = max(max_tool_wear, 1.0)
    risk_score = min(100.0, (avg_tool_wear / denominator) * 100.0)

    col1, col2, col3 = st.columns(3)
    col1.metric("Registros", f"{total_records:,}")
    col2.metric("Produtos", f"{total_products:,}")
    col3.metric("Tipos", f"{total_types:,}")

    gauge_col, bar_col = st.columns((1, 1))
    with gauge_col:
        render_gauge(risk_score=risk_score, avg_tool_wear=avg_tool_wear)
    with bar_col:
        bar_fig = px.bar(
            failure_df,
            x="failure_type",
            y="total_failures",
            color="failure_type",
            title="Distribuição de Falhas",
            labels={"failure_type": "Tipo de Falha", "total_failures": "Ocorrências"},
        )
        bar_fig.update_layout(showlegend=False, height=320)
        st.plotly_chart(bar_fig, use_container_width=True)

    st.subheader("Análise de Correlação")
    if corr_df.empty:
        st.info("Nenhum dado encontrado para os filtros selecionados.")
    else:
        heatmap_fig = px.density_heatmap(
            corr_df,
            x="air_temperature_k",
            y="torque_nm",
            z="frequency",
            histfunc="sum",
            nbinsx=30,
            nbinsy=30,
            color_continuous_scale="YlOrRd",
            title="Heatmap: Air temperature x Torque",
            labels={
                "air_temperature_k": "Air temperature [K]",
                "torque_nm": "Torque [Nm]",
                "frequency": "Frequência",
            },
        )
        heatmap_fig.update_layout(height=500)
        st.plotly_chart(heatmap_fig, use_container_width=True)

    with st.expander("Visualizar dados agregados da consulta"):
        st.dataframe(failure_df, use_container_width=True)
        st.dataframe(corr_df.head(1000), use_container_width=True)


if __name__ == "__main__":
    main()
