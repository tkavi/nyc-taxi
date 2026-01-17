-- Step 2: Validation of Trips
SELECT *
FROM standardized_nyc_taxi_data
WHERE 
    -- Dropoff must be after pickup 
    tpep_pickup_datetime < tpep_dropoff_datetime
    -- Ignore dates before mentioned date
    AND tpep_pickup_datetime BETWEEN '2000-01-01' AND CURRENT_TIMESTAMP()
    -- Standard TLC Taxi Zones (1-263) 
    AND PULocationID BETWEEN 1 AND 263 
    AND DOLocationID BETWEEN 1 AND 263;