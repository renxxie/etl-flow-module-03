import os
import json
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from sqlalchemy import text

def extract_transform_load():
    mongo_hook = MongoHook(mongo_conn_id='mongo_default')
    pg_hook = PostgresHook(postgres_conn_id='postgres_dwh')
    mongo_client = mongo_hook.get_conn()
    db = mongo_client.source_db
    
    pg_engine = pg_hook.get_sqlalchemy_engine()

    def safe_load(df, table_name, pk_col):
        if df.empty:
            return
        
        ids = list(df[pk_col].unique())
        
        with pg_engine.begin() as conn:
            conn.execute(
                text(f"DELETE FROM ods.{table_name} WHERE {pk_col} = ANY(:ids)"),
                {"ids": ids}
            )
            df.to_sql(table_name, conn, schema='ods', if_exists='append', index=False)

    sessions = list(db.UserSessions.find({}, {"_id": 0}))
    if sessions:
        df = pd.DataFrame(sessions)
        df.drop_duplicates(subset=['session_id'], inplace=True)
        df['pages_visited'] = df['pages_visited'].apply(lambda x: x if isinstance(x, list) else [])
        df['actions'] = df['actions'].apply(lambda x: x if isinstance(x, list) else [])
        safe_load(df, 'user_sessions', 'session_id')
        db.UserSessions.delete_many({"session_id": {"$in": df['session_id'].tolist()}})

    events = list(db.EventLogs.find({}, {"_id": 0}))
    if events:
        df = pd.DataFrame(events)
        df.drop_duplicates(subset=['event_id'], inplace=True)
        df['details'] = df['details'].astype(str)
        safe_load(df, 'event_logs', 'event_id')
        db.EventLogs.delete_many({"event_id": {"$in": df['event_id'].tolist()}})

    tickets = list(db.SupportTickets.find({}, {"_id": 0}))
    if tickets:
        df = pd.DataFrame(tickets)
        df.drop_duplicates(subset=['ticket_id'], inplace=True)
        df['messages'] = df['messages'].apply(lambda x: json.dumps(x, default=str))
        safe_load(df, 'support_tickets', 'ticket_id')
        db.SupportTickets.delete_many({"ticket_id": {"$in": df['ticket_id'].tolist()}})

    recs = list(db.UserRecommendations.find({}, {"_id": 0}))
    if recs:
        df = pd.DataFrame(recs)
        df.drop_duplicates(subset=['user_id'], inplace=True)
        safe_load(df, 'user_recommendations', 'user_id')
        db.UserRecommendations.delete_many({"user_id": {"$in": df['user_id'].tolist()}})

    mod_queue = list(db.ModerationQueue.find({}, {"_id": 0}))
    if mod_queue:
        df = pd.DataFrame(mod_queue)
        df.drop_duplicates(subset=['review_id'], inplace=True)
        safe_load(df, 'moderation_queue', 'review_id')
        db.ModerationQueue.delete_many({"review_id": {"$in": df['review_id'].tolist()}})


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
}

with DAG(
    '2_mongodb_to_postgres_etl',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    description='Copy from MongoDB and load to psql ods'
) as dag:
    
    etl_task = PythonOperator(
        task_id='extract_transform_load',
        python_callable=extract_transform_load
    )