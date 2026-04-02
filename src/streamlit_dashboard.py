import os
from typing import Optional, Tuple

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from databricks import sql

st.set_page_config(page_title="Smart Maintenance Dashboard", page_icon="🛠️", layout="wide")

DEFAULT_CATALOG = "hive_metastore"
DEFAULT_SCHEMA = "smart_maintenance"


def _get_secret(name: str, default: Optional[str] = None) -> Optional[str]:
    if name in st.secrets:
        return st.secrets[name]
    return os.getenv(name, default)


def get_connection() -> sql.Connection:
    hostname = _get_secret("DATABRICKS_SERVER_HOSTNAME")
    http_path = _get_secret("DATABRICKS_HTTP_PATH")
    access_token = _get_secret("DATABRICKS_ACCESS_TOKEN")

    if not hostname or not http_path or not access_token:
        raise RuntimeError(
            "Configure DATABRICKS_SERVER_HOSTNAME, DATABRICKS_HTTP_PATH e DATABRICKS_ACCESS_TOKEN em .streamlit/secrets.toml ou variáveis de ambiente."
        )

    return sql.connect(
        server_hostname=hostname,
        http_path=http_path,
        access_token=access_token,
    )


@st.cache_data(ttl=300)
def run_query(query: str, params: Optional[Tuple] = None) -> pd.DataFrame:
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, params=params)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
    return pd.DataFrame(rows, columns=columns)


@st.cache_data(ttl=300)
def get_filter_options(catalog: str, schema: str) -> tuple[list[str], list[str]]:
    query = f"""
    SELECT DISTINCT product_id, machine_type
    FROM {catalog}.{schema}.gold_ai4i2020_features
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


def main() -> None:
    st.title("Smart Maintenance Lakehouse Dashboard")
    st.caption("Streamlit conectado diretamente ao SQL Warehouse do Databricks via databricks-sql-python.")

    catalog = _get_secret("AI4I_TARGET_CATALOG", DEFAULT_CATALOG)
    schema = _get_secret("AI4I_TARGET_SCHEMA", DEFAULT_SCHEMA)

    try:
        product_ids, machine_types = get_filter_options(catalog, schema)
    except Exception as exc:
        st.error(str(exc))
        st.stop()

    with st.sidebar:
        st.header("Filtros Dinâmicos")
        selected_product_id = st.selectbox("Product ID", ["Todos"] + product_ids)
        selected_machine_type = st.selectbox("Type", ["Todos"] + machine_types)
        st.markdown("A conexão usa o SQL Warehouse do Databricks para manter a visualização desacoplada do cluster de processamento.")

    where_clause, params = build_filter_clause(selected_product_id, selected_machine_type)

    health_query = f"""
    SELECT
        COALESCE(AVG(tool_wear_min), 0) AS avg_tool_wear,
        COALESCE(MAX(tool_wear_min), 0) AS max_tool_wear,
        COUNT(*) AS total_records
    FROM {catalog}.{schema}.gold_ai4i2020_features
    {where_clause}
    """
    health_df = run_query(health_query, tuple(params))
    avg_tool_wear = float(health_df.iloc[0]["avg_tool_wear"]) if not health_df.empty else 0.0
    max_tool_wear = float(health_df.iloc[0]["max_tool_wear"]) if not health_df.empty else 0.0
    denominator = max(max_tool_wear, 1.0)
    risk_score = min(100.0, (avg_tool_wear / denominator) * 100.0)

    corr_query = f"""
    SELECT
        ROUND(air_temperature_k, 1) AS air_temperature_k,
        ROUND(torque_nm, 1) AS torque_nm,
        COUNT(*) AS frequency
    FROM {catalog}.{schema}.gold_ai4i2020_features
    {where_clause}
    GROUP BY ROUND(air_temperature_k, 1), ROUND(torque_nm, 1)
    ORDER BY air_temperature_k, torque_nm
    """
    corr_df = run_query(corr_query, tuple(params))

    failure_query = f"""
    SELECT 'TWF' AS failure_type, COALESCE(SUM(twf_label), 0) AS total_failures
    FROM {catalog}.{schema}.gold_ai4i2020_features
    {where_clause}
    UNION ALL
    SELECT 'HDF' AS failure_type, COALESCE(SUM(hdf_label), 0) AS total_failures
    FROM {catalog}.{schema}.gold_ai4i2020_features
    {where_clause}
    UNION ALL
    SELECT 'PWF' AS failure_type, COALESCE(SUM(pwf_label), 0) AS total_failures
    FROM {catalog}.{schema}.gold_ai4i2020_features
    {where_clause}
    UNION ALL
    SELECT 'OSF' AS failure_type, COALESCE(SUM(osf_label), 0) AS total_failures
    FROM {catalog}.{schema}.gold_ai4i2020_features
    {where_clause}
    UNION ALL
    SELECT 'RNF' AS failure_type, COALESCE(SUM(rnf_label), 0) AS total_failures
    FROM {catalog}.{schema}.gold_ai4i2020_features
    {where_clause}
    """
    failure_params = tuple(params + params + params + params + params)
    failure_df = run_query(failure_query, failure_params)

    summary_query = f"""
    SELECT
        COUNT(*) AS total_records,
        COUNT(DISTINCT product_id) AS total_products,
        COUNT(DISTINCT machine_type) AS total_types
    FROM {catalog}.{schema}.gold_ai4i2020_features
    {where_clause}
    """
    summary_df = run_query(summary_query, tuple(params))

    total_records = int(summary_df.iloc[0]["total_records"]) if not summary_df.empty else 0
    total_products = int(summary_df.iloc[0]["total_products"]) if not summary_df.empty else 0
    total_types = int(summary_df.iloc[0]["total_types"]) if not summary_df.empty else 0

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
