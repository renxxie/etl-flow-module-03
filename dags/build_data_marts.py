from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
}

create_user_activity_mart = """
    BEGIN;
    DROP TABLE IF EXISTS dm.user_activity_mart CASCADE;
    CREATE TABLE dm.user_activity_mart AS
    SELECT 
        user_id,
        COUNT(session_id) AS total_sessions,
        AVG(EXTRACT(EPOCH FROM (end_time - start_time))/60.0) AS avg_session_duration_minutes,
        MAX(start_time) AS last_active_time
    FROM ods.user_sessions
    GROUP BY user_id;
    COMMIT;
"""

create_support_efficiency_mart = """
    BEGIN;
    DROP TABLE IF EXISTS dm.support_efficiency_mart CASCADE;
    CREATE TABLE dm.support_efficiency_mart AS
    SELECT 
        issue_type,
        status,
        COUNT(ticket_id) AS total_tickets,
        AVG(EXTRACT(EPOCH FROM (updated_at - created_at))/3600.0) AS avg_resolution_hours,
        MAX(updated_at) AS last_updated_time
    FROM ods.support_tickets
    GROUP BY issue_type, status;
    COMMIT;
"""

with DAG(
    '3_build_data_marts',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    description='Data Marts in psql'
) as dag:
    
    build_user_activity = PostgresOperator(
        task_id='build_user_activity_mart',
        postgres_conn_id='postgres_dwh',
        sql=create_user_activity_mart
    )

    build_support_efficiency = PostgresOperator(
        task_id='build_support_efficiency_mart',
        postgres_conn_id='postgres_dwh',
        sql=create_support_efficiency_mart
    )

    build_user_activity >> build_support_efficiency