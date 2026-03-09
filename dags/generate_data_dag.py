import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from faker import Faker
import random

def generate_data():
    hook = MongoHook(mongo_conn_id='mongo_default')
    client = hook.get_conn()
    db = client.source_db
    fake = Faker()
    
    user_sessions = []
    for _ in range(100):
        start_time = fake.date_time_between(start_date='-30d', end_date='now')
        user_sessions.append({
            "session_id": f"sess_{fake.uuid4()}",
            "user_id": f"user_{random.randint(1, 1000)}",
            "start_time": start_time,
            "end_time": fake.date_time_between(start_date=start_time, end_date='now'),
            "pages_visited": [fake.uri_path() for _ in range(random.randint(1, 10))],
            "device": random.choice(["mobile", "desktop", "tablet"]),
            "actions": [random.choice(["login", "view_product", "add_to_cart", "logout", "purchase"]) for _ in range(random.randint(2, 8))]
        })
    if user_sessions:
        db.UserSessions.insert_many(user_sessions)

    event_logs = []
    for _ in range(200):
        event_logs.append({
            "event_id": f"evt_{fake.uuid4()}",
            "timestamp": fake.date_time_between(start_date='-30d', end_date='now'),
            "event_type": random.choice(["click", "scroll", "hover", "input", "submit"]),
            "details": fake.sentence()
        })
    if event_logs:
        db.EventLogs.insert_many(event_logs)

    support_tickets = []
    for _ in range(50):
        created_at = fake.date_time_between(start_date='-30d', end_date='now')
        support_tickets.append({
            "ticket_id": f"ticket_{fake.uuid4()}",
            "user_id": f"user_{random.randint(1, 1000)}",
            "status": random.choice(["open", "closed", "in_progress", "resolved"]),
            "issue_type": random.choice(["payment", "delivery", "account", "product", "other"]),
            "messages": [
                {
                    "sender": "user",
                    "message": fake.sentence(),
                    "timestamp": created_at
                },
                {
                    "sender": "support",
                    "message": fake.sentence(),
                    "timestamp": fake.date_time_between(start_date=created_at, end_date='now')
                }
            ],
            "created_at": created_at,
            "updated_at": fake.date_time_between(start_date=created_at, end_date='now')
        })
    if support_tickets:
        db.SupportTickets.insert_many(support_tickets)

    user_recommendations = []
    for i in range(1, 51):
        user_recommendations.append({
            "user_id": f"user_{i}",
            "recommended_products": [f"prod_{random.randint(1, 500)}" for _ in range(random.randint(1, 5))],
            "last_updated": fake.date_time_between(start_date='-30d', end_date='now')
        })
    if user_recommendations:
        for hr in user_recommendations:
            db.UserRecommendations.update_one({'user_id': hr['user_id']}, {'$set': hr}, upsert=True)

    moderation_queue = []
    for _ in range(60):
        moderation_queue.append({
            "review_id": f"rev_{fake.uuid4()}",
            "user_id": f"user_{random.randint(1, 1000)}",
            "product_id": f"prod_{random.randint(1, 500)}",
            "review_text": fake.text(),
            "rating": random.randint(1, 5),
            "moderation_status": random.choice(["pending", "approved", "rejected"]),
            "flags": [random.choice(["contains_images", "spam", "profanity"]) for _ in range(random.randint(0, 2))],
            "submitted_at": fake.date_time_between(start_date='-30d', end_date='now')
        })
    if moderation_queue:
        db.ModerationQueue.insert_many(moderation_queue)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
}

with DAG(
    '1_generate_data_mongodb',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    description='Generate data and load to MongoDB'
) as dag:
    
    generate_data_task = PythonOperator(
        task_id='generate_data_to_mongo',
        python_callable=generate_data
    )