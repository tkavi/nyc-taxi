-- Step 1: Standardize 
SELECT 
    VendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    CAST(COALESCE(passenger_count,0) AS INT) AS passenger_count,
    COALESCE(trip_distance,0),
    -- RatecodeID NULL to 99 as per dictionary 
    CASE 
        WHEN RatecodeID IS NULL THEN 99 
        ELSE CAST(RatecodeID AS INT) 
    END AS RatecodeID,
    -- Other value to NULL 
    CASE 
        WHEN TRIM(UPPER(store_and_fwd_flag)) = 'Y' THEN 'Y' 
        WHEN TRIM(UPPER(store_and_fwd_flag)) = 'N' THEN 'N' 
        ELSE NULL 
    END AS store_and_fwd_flag,
    PULocationID,
    DOLocationID,
    CAST(payment_type AS INT) AS payment_type,
    COALESCE(fare_amount,0)
    COALESCE(extra,0),
    COALESCE(mta_tax,0),
    COALESCE(tip_amount,0),
    COALESCE(tolls_amount,0),
    COALESCE(improvement_surcharge,0),
    COALESCE(total_amount,0),
    COALESCE(congestion_surcharge,0),
    COALESCE(airport_fee,0),
    COALESCE(cbd_congestion_fee,0)
FROM raw_nyc_taxi_data
WHERE 
    -- Valid TPEP providers
    VendorID IN (1, 2, 6, 7) 
    AND VendorID IS NOT NULL;