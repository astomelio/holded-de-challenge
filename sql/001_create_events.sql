-- Create events table for Holded data pipeline
-- This table stores processed events from the silver layer

CREATE TABLE IF NOT EXISTS events (
    event_id VARCHAR(255) PRIMARY KEY,
    event_type VARCHAR(255) NOT NULL, -- Increased size to accommodate long Holded event types
    user_id VARCHAR(255),
    company_id VARCHAR(255) NOT NULL,
    timestamp BIGINT NOT NULL,
    processed_at BIGINT NOT NULL,
    ip_address VARCHAR(45),
    user_agent TEXT,
    session_id VARCHAR(255),
    data_quality_score DECIMAL(3,2),
    partition_key VARCHAR(255),
    business_context VARCHAR(50),
    metadata JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX idx_events_company_id ON events(company_id);
CREATE INDEX idx_events_event_type ON events(event_type);
CREATE INDEX idx_events_timestamp ON events(timestamp);
CREATE INDEX idx_events_user_id ON events(user_id);
CREATE INDEX idx_events_business_context ON events(business_context);
CREATE INDEX idx_events_created_at ON events(created_at);

-- Create a partitioned table for better performance (if using MySQL 8.0+)
-- ALTER TABLE events PARTITION BY RANGE (UNIX_TIMESTAMP(created_at)) (
--     PARTITION p_2024_01 VALUES LESS THAN (UNIX_TIMESTAMP('2024-02-01')),
--     PARTITION p_2024_02 VALUES LESS THAN (UNIX_TIMESTAMP('2024-03-01')),
--     PARTITION p_2024_03 VALUES LESS THAN (UNIX_TIMESTAMP('2024-04-01')),
--     PARTITION p_future VALUES LESS THAN MAXVALUE
-- );

-- Create events summary table for analytics
CREATE TABLE IF NOT EXISTS events_summary (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    company_id VARCHAR(255) NOT NULL,
    event_type VARCHAR(255) NOT NULL, -- Increased size to accommodate long Holded event types
    business_context VARCHAR(50) NOT NULL,
    event_count INT NOT NULL DEFAULT 0,
    first_event_timestamp BIGINT,
    last_event_timestamp BIGINT,
    avg_data_quality_score DECIMAL(3,2),
    date_hour TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY unique_summary (company_id, event_type, business_context, date_hour)
);

-- Create index for events summary
CREATE INDEX idx_events_summary_company_id ON events_summary(company_id);
CREATE INDEX idx_events_summary_date_hour ON events_summary(date_hour);
CREATE INDEX idx_events_summary_business_context ON events_summary(business_context);

-- Create dead letter queue table for invalid events
CREATE TABLE IF NOT EXISTS events_dead_letter (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_id VARCHAR(255),
    raw_event JSON NOT NULL,
    error_message TEXT,
    error_type VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP NULL
);

-- Create index for dead letter queue
CREATE INDEX idx_events_dead_letter_event_id ON events_dead_letter(event_id);
CREATE INDEX idx_events_dead_letter_created_at ON events_dead_letter(created_at);
CREATE INDEX idx_events_dead_letter_error_type ON events_dead_letter(error_type);
