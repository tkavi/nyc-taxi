-- Step 1: Standardize 
SELECT 
    VendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    -- RatecodeID 99 to NULL as per dictionary 
    CASE 
        WHEN RatecodeID = 99 THEN NULL 
        ELSE CAST(RatecodeID AS INT) 
    END AS RatecodeID,
    -- Other value to NULL 
    CASE 
        WHEN store_and_fwd_flag = 'Y' THEN 'Y' 
        WHEN store_and_fwd_flag = 'N' THEN 'N' 
        ELSE NULL 
    END AS store_and_fwd_flag,
    PULocationID,
    DOLocationID,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee
FROM raw_nyc_taxi_data
WHERE 
    -- Valid TPEP providers
    VendorID IN (1, 2, 6, 7) 
    AND VendorID IS NOT NULL;