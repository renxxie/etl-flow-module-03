CREATE DATABASE dwh_db;
CREATE USER dwh_user WITH PASSWORD 'dwh_password';
ALTER DATABASE dwh_db OWNER TO dwh_user;
GRANT ALL PRIVILEGES ON DATABASE dwh_db TO dwh_user;

\c dwh_db dwh_user;

CREATE SCHEMA IF NOT EXISTS ods;
CREATE SCHEMA IF NOT EXISTS dm;

CREATE TABLE IF NOT EXISTS ods.user_sessions (
    session_id VARCHAR(50),
    user_id VARCHAR(50),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    pages_visited TEXT[],
    device VARCHAR(50),
    actions TEXT[],
    PRIMARY KEY (session_id, start_time)
) PARTITION BY RANGE (start_time);

CREATE TABLE ods.user_sessions_y2024 PARTITION OF ods.user_sessions
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE ods.user_sessions_y2025 PARTITION OF ods.user_sessions
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE ods.user_sessions_y2026 PARTITION OF ods.user_sessions
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE IF NOT EXISTS ods.event_logs (
    event_id VARCHAR(50),
    timestamp TIMESTAMP,
    event_type VARCHAR(50),
    details TEXT,
    PRIMARY KEY (event_id, timestamp)
) PARTITION BY RANGE (timestamp);

CREATE TABLE ods.event_logs_y2024 PARTITION OF ods.event_logs
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE ods.event_logs_y2025 PARTITION OF ods.event_logs
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE ods.event_logs_y2026 PARTITION OF ods.event_logs
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE IF NOT EXISTS ods.support_tickets (
    ticket_id VARCHAR(50),
    user_id VARCHAR(50),
    status VARCHAR(50),
    issue_type VARCHAR(50),
    messages JSONB,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    PRIMARY KEY (ticket_id, created_at)
) PARTITION BY RANGE (created_at);

CREATE TABLE ods.support_tickets_y2024 PARTITION OF ods.support_tickets
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE ods.support_tickets_y2025 PARTITION OF ods.support_tickets
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE ods.support_tickets_y2026 PARTITION OF ods.support_tickets
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE IF NOT EXISTS ods.user_recommendations (
    user_id VARCHAR(50) PRIMARY KEY,
    recommended_products TEXT[],
    last_updated TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ods.moderation_queue (
    review_id VARCHAR(50),
    user_id VARCHAR(50),
    product_id VARCHAR(50),
    review_text TEXT,
    rating INT,
    moderation_status VARCHAR(50),
    flags TEXT[],
    submitted_at TIMESTAMP,
    PRIMARY KEY (review_id, submitted_at)
) PARTITION BY RANGE (submitted_at);

CREATE TABLE ods.moderation_queue_y2024 PARTITION OF ods.moderation_queue
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE ods.moderation_queue_y2025 PARTITION OF ods.moderation_queue
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE ods.moderation_queue_y2026 PARTITION OF ods.moderation_queue
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');