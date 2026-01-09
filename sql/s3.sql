/*
  Project: NYC Taxi
  Layer: Raw to Processed
  Author: Kavita | Version: 1.0.1
  Governance: Internal-only, PII-masked
*/

SELECT 
    UPPER(vendor_id) as vendor_id,
    CAST(tpep_pickup_datetime AS TIMESTAMP) as pickup_time,
    CAST(fare_amount AS DOUBLE) as fare,
    (fare_amount + tip_amount) as total_collected
FROM raw_table
WHERE fare_amount > 0;