-- Step 3: Financial Sanity, Logical Constraints, and Fee Validation
SELECT *
FROM validated_nyc_taxi_data
WHERE 
    -- 1. Global Financial Sanity: All core amounts must be non-negative 
    fare_amount >= 0 
    AND total_amount >= 0 
    AND tip_amount >= 0
    AND extra >= 0
    AND mta_tax >= 0
    AND tolls_amount >= 0
    AND improvement_surcharge >= 0

    -- 2. Logical Revenue Constraints
    -- A trip with a fare must have a recorded distance 
    AND (CASE WHEN fare_amount > 0 THEN trip_distance > 0 ELSE TRUE END)
    -- Revenue trips must have at least one passenger 
    AND passenger_count > 0

    -- 3. Payment & Surcharge Validation
    -- Payment types must be within the defined 0-6 range 
    AND payment_type BETWEEN 0 AND 6
    -- New CBD congestion fee (started Jan 2025) must be valid if present 
    AND (cbd_congestion_fee >= 0 OR cbd_congestion_fee IS NULL)
    -- Congestion surcharge must be non-negative 
    AND congestion_surcharge >= 0;