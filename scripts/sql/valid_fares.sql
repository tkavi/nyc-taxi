-- Step 3: Validation of Fares
SELECT *
FROM validated_nyc_taxi_data
WHERE 
    -- All amounts must be non-negative 
    fare_amount >= 0 
    AND total_amount >= 0 
    AND tip_amount >= 0
    AND extra >= 0
    AND mta_tax >= 0
    AND tolls_amount >= 0
    AND improvement_surcharge >= 0
    AND (CASE WHEN fare_amount > 0 THEN trip_distance > 0 ELSE TRUE END)
    AND passenger_count > 0
    -- Payment types must be within 0-6 
    AND payment_type BETWEEN 0 AND 6
    AND (cbd_congestion_fee >= 0 OR cbd_congestion_fee IS NULL)
    AND congestion_surcharge >= 0;