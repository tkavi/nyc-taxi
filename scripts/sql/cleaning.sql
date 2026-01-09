/*
  Project: NYC Taxi
  Layer: Raw to Processed
  Description: Standardizes column types and filters nulls.
  Author: Kavita| Version: 1.
*/

SELECT 
    vendor_id,
    CAST(tpep_pickup_datetime AS TIMESTAMP) as pickup_time,
    CAST(tpep_dropoff_datetime AS TIMESTAMP) as dropoff_time,
    CAST(fare_amount AS DOUBLE) as fare,
    passenger_count,
    trip_distance
FROM raw_nyc_taxi_data
WHERE vendor_id IS NOT NULL;