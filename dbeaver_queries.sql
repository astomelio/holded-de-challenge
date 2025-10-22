-- Holded Data Pipeline - DBeaver Demo Queries
-- Use these queries in DBeaver to demonstrate the system

-- Connection details for DBeaver:
-- Host: localhost
-- Port: 3306
-- Database: holded_events
-- Username: holded
-- Password: holded123

-- 1. Total events count
SELECT COUNT(*) as total_events FROM events;

-- 2. Events by type
SELECT 
    event_type,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM events), 2) as percentage
FROM events 
GROUP BY event_type 
ORDER BY count DESC;

-- 3. Events by company
SELECT 
    company_id,
    COUNT(*) as event_count,
    MIN(FROM_UNIXTIME(timestamp/1000)) as first_event,
    MAX(FROM_UNIXTIME(timestamp/1000)) as last_event
FROM events 
GROUP BY company_id 
ORDER BY event_count DESC
LIMIT 10;

-- 4. Recent events (last 10)
SELECT 
    event_id,
    event_type,
    company_id,
    FROM_UNIXTIME(timestamp/1000) as event_time,
    FROM_UNIXTIME(processed_at/1000) as processed_time,
    data_quality_score
FROM events 
ORDER BY timestamp DESC 
LIMIT 10;

-- 5. Events summary by hour
SELECT 
    DATE_FORMAT(FROM_UNIXTIME(timestamp/1000), '%Y-%m-%d %H:00:00') as hour,
    COUNT(*) as events_count,
    COUNT(DISTINCT company_id) as unique_companies,
    AVG(data_quality_score) as avg_quality
FROM events 
GROUP BY DATE_FORMAT(FROM_UNIXTIME(timestamp/1000), '%Y-%m-%d %H:00:00')
ORDER BY hour DESC
LIMIT 10;

-- 6. Data quality analysis
SELECT 
    CASE 
        WHEN data_quality_score >= 0.9 THEN 'Excellent'
        WHEN data_quality_score >= 0.7 THEN 'Good'
        WHEN data_quality_score >= 0.5 THEN 'Fair'
        ELSE 'Poor'
    END as quality_category,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM events), 2) as percentage
FROM events 
WHERE data_quality_score IS NOT NULL
GROUP BY 
    CASE 
        WHEN data_quality_score >= 0.9 THEN 'Excellent'
        WHEN data_quality_score >= 0.7 THEN 'Good'
        WHEN data_quality_score >= 0.5 THEN 'Fair'
        ELSE 'Poor'
    END
ORDER BY count DESC;

-- 7. Business context analysis
SELECT 
    business_context,
    COUNT(*) as count,
    COUNT(DISTINCT company_id) as unique_companies
FROM events 
WHERE business_context IS NOT NULL
GROUP BY business_context 
ORDER BY count DESC;

-- 8. Events summary table
SELECT * FROM events_summary ORDER BY date_hour DESC LIMIT 10;

-- 9. Dead letter queue (if any errors)
SELECT 
    error_type,
    COUNT(*) as error_count,
    MAX(created_at) as last_error
FROM events_dead_letter 
GROUP BY error_type 
ORDER BY error_count DESC;

-- 10. System performance metrics
SELECT 
    'Total Events' as metric,
    COUNT(*) as value
FROM events
UNION ALL
SELECT 
    'Unique Companies',
    COUNT(DISTINCT company_id)
FROM events
UNION ALL
SELECT 
    'Event Types',
    COUNT(DISTINCT event_type)
FROM events
UNION ALL
SELECT 
    'Avg Quality Score',
    ROUND(AVG(data_quality_score), 3)
FROM events
WHERE data_quality_score IS NOT NULL;
