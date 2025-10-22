-- PostgreSQL Business Tables
-- This file creates the business data tables in PostgreSQL

-- Create database if not exists
CREATE DATABASE holded_business;

-- Connect to the database
\c holded_business;

-- Business events table
CREATE TABLE IF NOT EXISTS business_events (
    event_id VARCHAR(255) PRIMARY KEY,
    event_type VARCHAR(255) NOT NULL,
    company_id VARCHAR(255) NOT NULL,
    user_id VARCHAR(255),
    timestamp BIGINT NOT NULL,
    business_context VARCHAR(50),
    data_quality_score DECIMAL(3,2),
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Company analytics table
CREATE TABLE IF NOT EXISTS company_analytics (
    company_id VARCHAR(255) PRIMARY KEY,
    total_events INTEGER DEFAULT 0,
    last_event_timestamp BIGINT,
    avg_data_quality DECIMAL(3,2),
    business_contexts JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Event type summary table
CREATE TABLE IF NOT EXISTS event_type_summary (
    event_type VARCHAR(255) PRIMARY KEY,
    total_count INTEGER DEFAULT 0,
    unique_companies INTEGER DEFAULT 0,
    avg_quality_score DECIMAL(3,2),
    last_occurrence TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_business_events_company_id ON business_events(company_id);
CREATE INDEX IF NOT EXISTS idx_business_events_timestamp ON business_events(timestamp);
CREATE INDEX IF NOT EXISTS idx_business_events_business_context ON business_events(business_context);
CREATE INDEX IF NOT EXISTS idx_business_events_data_quality ON business_events(data_quality_score);

-- Create triggers for automatic updates
CREATE OR REPLACE FUNCTION update_company_analytics()
RETURNS TRIGGER AS $$
BEGIN
    -- Update company analytics
    INSERT INTO company_analytics (
        company_id, total_events, last_event_timestamp, 
        avg_data_quality, business_contexts
    ) VALUES (
        NEW.company_id, 1, NEW.timestamp, 
        NEW.data_quality_score, 
        jsonb_build_object(NEW.business_context, 1)
    )
    ON CONFLICT (company_id) DO UPDATE SET
        total_events = company_analytics.total_events + 1,
        last_event_timestamp = GREATEST(company_analytics.last_event_timestamp, NEW.timestamp),
        avg_data_quality = (company_analytics.avg_data_quality * (company_analytics.total_events - 1) + NEW.data_quality_score) / company_analytics.total_events,
        business_contexts = company_analytics.business_contexts || jsonb_build_object(NEW.business_context, COALESCE(company_analytics.business_contexts->>NEW.business_context, '0')::int + 1),
        updated_at = CURRENT_TIMESTAMP;
    
    -- Update event type summary
    INSERT INTO event_type_summary (
        event_type, total_count, unique_companies, 
        avg_quality_score, last_occurrence
    ) VALUES (
        NEW.event_type, 1, 1, 
        NEW.data_quality_score, 
        to_timestamp(NEW.timestamp / 1000)
    )
    ON CONFLICT (event_type) DO UPDATE SET
        total_count = event_type_summary.total_count + 1,
        unique_companies = (SELECT COUNT(DISTINCT company_id) FROM business_events WHERE event_type = NEW.event_type),
        avg_quality_score = (event_type_summary.avg_quality_score * (event_type_summary.total_count - 1) + NEW.data_quality_score) / event_type_summary.total_count,
        last_occurrence = to_timestamp(NEW.timestamp / 1000),
        updated_at = CURRENT_TIMESTAMP;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger
DROP TRIGGER IF EXISTS trigger_update_analytics ON business_events;
CREATE TRIGGER trigger_update_analytics
    AFTER INSERT ON business_events
    FOR EACH ROW
    EXECUTE FUNCTION update_company_analytics();

-- Insert sample data for testing
INSERT INTO business_events (
    event_id, event_type, company_id, user_id, timestamp,
    business_context, data_quality_score, metadata
) VALUES (
    'sample_001', 'Holded\\Wallet\\Domain\\Transaction\\Events\\WalletTransactionCreatedEvent',
    'company_001', 'user_001', 1640995200000,
    'financial', 0.95, '{"amount": 100.50, "currency": "EUR"}'
) ON CONFLICT (event_id) DO NOTHING;
