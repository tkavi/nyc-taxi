CREATE TABLE IF NOT EXISTS trip_details (
    vendorid INT ENCODE AZ64,
    vendor_name VARCHAR(50) ENCODE ZSTD,
    tpep_pickup_datetime TIMESTAMP ENCODE AZ64,
    tpep_dropoff_datetime TIMESTAMP ENCODE AZ64,
    pulocationid INT ENCODE AZ64,          
    pickup_zone VARCHAR(100) ENCODE ZSTD,
    pickup_borough VARCHAR(50) ENCODE ZSTD,
    dolocationid INT ENCODE AZ64,
    dropoff_zone VARCHAR(100) ENCODE ZSTD,
    dropoff_borough VARCHAR(50) ENCODE ZSTD,
    passenger_count INT ENCODE AZ64,
    trip_distance DOUBLE PRECISION ENCODE RAW,
    ratecodeid INT ENCODE AZ64,
    ratecode_desc VARCHAR(50) ENCODE ZSTD,
    fare_amount DECIMAL(10,2) ENCODE AZ64,
    extra DECIMAL(10,2) ENCODE AZ64,
    mta_tax DECIMAL(10,2) ENCODE AZ64,
    tip_amount DECIMAL(10,2) ENCODE AZ64,
    tolls_amount DECIMAL(10,2) ENCODE AZ64,
    improvement_surcharge DECIMAL(10,2) ENCODE AZ64,
    total_amount DECIMAL(10,2) ENCODE AZ64,
    congestion_surcharge DECIMAL(10,2) ENCODE AZ64,
    airport_fee DECIMAL(10,2) ENCODE AZ64,
    cbd_congestion_fee DECIMAL(10,2) ENCODE AZ64
) 
DISTSTYLE KEY DISTKEY (pulocationid) 
SORTKEY (tpep_pickup_datetime);