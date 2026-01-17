-- Step 1: Standardize 
SELECT 
    VendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    CAST(COALESCE(passenger_count,0) AS INT) AS passenger_count,
    COALESCE(trip_distance,0) AS trip_distance,
    -- RatecodeID NULL to 99 as per dictionary 
    CAST(COALESCE(RatecodeID,99) AS INT) AS RatecodeID,
    -- Other value to 'U'(Unknown) 
    CASE 
        WHEN TRIM(UPPER(store_and_fwd_flag)) = 'Y' THEN 'Y' 
        WHEN TRIM(UPPER(store_and_fwd_flag)) = 'N' THEN 'N' 
        ELSE 'U' 
    END AS store_and_fwd_flag,
    PULocationID,
    DOLocationID,
    CAST(payment_type AS INT) AS payment_type,
    COALESCE(fare_amount,0) AS fare_amount,
    COALESCE(extra,0) AS extra,
    COALESCE(mta_tax,0) AS mta_tax,
    COALESCE(tip_amount,0) AS tip_amount,
    COALESCE(tolls_amount,0) AS tolls_amount,
    COALESCE(improvement_surcharge,0) AS improvement_surcharge,
    COALESCE(total_amount,0) AS total_amount,
    COALESCE(congestion_surcharge,0) AS congestion_surcharge,
    COALESCE(airport_fee,0) AS airport_fee,
    COALESCE(cbd_congestion_fee,0) AS cbd_congestion_fee
FROM raw_nyc_taxi_data
WHERE 
    -- Valid TPEP providers
    VendorID IN (1, 2, 6, 7) 
    AND VendorID IS NOT NULL;