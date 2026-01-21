CREATE TABLE IF NOT EXISTS location_performance (
    pickup_zone VARCHAR(100) ENCODE ZSTD,
    pickup_borough VARCHAR(50) ENCODE ZSTD,
    pickup_count BIGINT ENCODE AZ64,
    avg_fare_by_zone DOUBLE PRECISION ENCODE RAW
) 
DISTSTYLE ALL 
SORTKEY (pickup_borough, pickup_zone); 