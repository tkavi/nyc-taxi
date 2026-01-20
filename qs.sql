-- Run this in your Redshift Query Editor
CREATE OR REPLACE VIEW public.v_governance_metrics AS
SELECT 
    'NYC Taxi Trips' as dataset_name,
    'Operations Team' as owner_label,
    -- This calculates your "99.2%" indicator dynamically
    ROUND(
        (COUNT(trip_id) - COUNT(CASE WHEN fare_amount IS NULL THEN 1 END))::FLOAT 
        / COUNT(trip_id) * 100, 2
    ) as quality_score,
    MAX(pickup_datetime) as last_updated
FROM curated.ny_taxi_trips;