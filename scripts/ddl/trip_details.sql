DROP TABLE IF EXISTS trip_details;

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
    fare_amount DOUBLE PRECISION ENCODE RAW,
    extra DOUBLE PRECISION ENCODE RAW,
    mta_tax DOUBLE PRECISION ENCODE RAW,
    tip_amount DOUBLE PRECISION ENCODE RAW,
    tolls_amount DOUBLE PRECISION ENCODE RAW,
    improvement_surcharge DOUBLE PRECISION ENCODE RAW,
    total_amount DOUBLE PRECISION ENCODE RAW,
    congestion_surcharge DOUBLE PRECISION ENCODE RAW,
    airport_fee DOUBLE PRECISION ENCODE RAW,
    cbd_congestion_fee DOUBLE PRECISION ENCODE RAW
) 
DISTSTYLE EVEN
SORTKEY (pulocationid, tpep_pickup_datetime);