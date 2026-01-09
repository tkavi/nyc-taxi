/*
  Project: NYC Taxi
  Layer: Raw to Processed
  Description: Casts types and filters baseline "bad" data.
  Author: Kavita | Version: 1.0
*/

SELECT 
    vendor_id,
    CAST(tpep_pickup_datetime AS TIMESTAMP) as tpep_pickup_datetime,
    CAST(tpep_dropoff_datetime AS TIMESTAMP) as tpep_dropoff_datetime,
    CAST(passenger_count AS INT) as passenger_count,
    CAST(trip_distance AS DOUBLE) as trip_distance,
    CAST(fare_amount AS DOUBLE) as fare,
    CAST(tip_amount AS DOUBLE) as tip_amount
FROM raw_landing_files -- This matches the view registered in PySpark
WHERE fare_amount > 0 
  AND passenger_count > 0;