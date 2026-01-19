CREATE TABLE IF NOT EXISTS vendor_performance (
    vendor_name VARCHAR(50) ENCODE ZSTD,
    total_trips BIGINT ENCODE AZ64,
    revenue DECIMAL(10,2) ENCODE AZ64
) 
DISTSTYLE ALL 
SORTKEY (vendor_name);