import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px
import os

st.set_page_config(page_title="DWH Dashboard", layout="wide")

DB_URL = "postgresql://dwh_user:dwh_password@postgres:5432/dwh_db"
engine = create_engine(DB_URL)

st.title("Мониторинг DWH (PostgreSQL)")

schema = st.sidebar.selectbox("Выберите схему", ["ods", "dm"])

if schema == "ods":
    tables = ["user_sessions", "event_logs", "support_tickets", "user_recommendations", "moderation_queue"]
else:
    tables = ["user_activity_mart", "support_efficiency_mart"]

table = st.sidebar.selectbox("Выберите таблицу", tables)

st.header(f"Содержимое таблицы: {schema}.{table}")

try:
    df = pd.read_sql(f"SELECT * FROM {schema}.{table} LIMIT 1000", engine)
    
    if df.empty:
        st.warning("Таблица пуста. Запустите ETL пайплайны в Airflow.")
    else:
        st.dataframe(df, use_container_width=True)
        st.write(f"Отображено записей: {len(df)}")

        if schema == "dm":
            st.divider()
            if table == "user_activity_mart":
                st.subheader("Активность пользователей (Топ-10)")
                fig = px.bar(df.head(10), x="user_id", y="total_sessions", 
                           title="Количество сессий по пользователям",
                           color="avg_session_duration_minutes")
                st.plotly_chart(fig, use_container_width=True)
            
            elif table == "support_efficiency_mart":
                st.subheader("Статистика поддержки")
                col1, col2 = st.columns(2)
                
                with col1:
                    fig_pie = px.pie(df, values="total_tickets", names="status", title="Распределение тикетов по статусам")
                    st.plotly_chart(fig_pie)
                
                with col2:
                    fig_box = px.bar(df, x="issue_type", y="avg_resolution_hours", color="status",
                                   title="Среднее время решения (часы)")
                    st.plotly_chart(fig_box)

except Exception as e:
    st.error(f"Ошибка при чтении данных: {e}")
    st.info("Убедитесь, что миграции выполнены и данные загружены.")

if st.button("Обновить данные"):
    st.rerun()