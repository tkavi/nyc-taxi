-- Step 2: Validate pickup/dropoff times and taxi zones
SELECT *
FROM standardized_nyc_taxi_data
WHERE 
    -- Dropoff must be after pickup 
    CAST(tpep_pickup_datetime AS TIMESTAMP) < CAST(tpep_dropoff_datetime AS TIMESTAMP)
    -- Ignore dates before mentioned date
    AND CAST(tpep_pickup_datetime AS TIMESTAMP) BETWEEN '2000-01-01' AND CURRENT_TIMESTAMP()
    -- Standard TLC Taxi Zones (1-263) 
    AND PULocationID BETWEEN 1 AND 263 
    AND DOLocationID BETWEEN 1 AND 263;