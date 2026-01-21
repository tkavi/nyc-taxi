CREATE TABLE IF NOT EXISTS vendor_performance (
    vendor_name VARCHAR(50) ENCODE ZSTD,
    total_trips BIGINT ENCODE AZ64,
    revenue DOUBLE PRECISION ENCODE RAW
) 
DISTSTYLE ALL 
SORTKEY (vendor_name);