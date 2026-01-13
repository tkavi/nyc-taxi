-- Step 1: Standardize technical flags and verify Vendor IDs
SELECT 
    *,
    -- Convert Y/N to Boolean as per dictionary store_and_fwd_flag 
    CASE 
        WHEN store_and_fwd_flag = 'Y' THEN true 
        WHEN store_and_fwd_flag = 'N' THEN false 
        ELSE NULL 
    END AS store_and_fwd_flag,
    -- Map RatecodeID 99 to NULL as per dictionary 
    CASE 
        WHEN RatecodeID = 99 THEN NULL 
        ELSE CAST(RatecodeID AS INT) 
    END AS RatecodeID
FROM raw_nyc_taxi_data
WHERE 
    -- Authorized TPEP providers: 1, 2, 6, 7 
    VendorID IN (1, 2, 6, 7) 
    AND VendorID IS NOT NULL;