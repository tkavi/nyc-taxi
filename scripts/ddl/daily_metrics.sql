CREATE TABLE IF NOT EXISTS daily_metrics (
    trip_date DATE ENCODE AZ64,              
    vendor_name VARCHAR(50) ENCODE ZSTD,
    total_trips BIGINT ENCODE AZ64,
    daily_revenue_mln DECIMAL(10,2) ENCODE AZ64, 
    avg_tip DECIMAL(10,2) ENCODE AZ64,
    avg_tip_percentage DOUBLE PRECISION ENCODE RAW, 
    longest_trip DOUBLE PRECISION ENCODE RAW
) 
DISTSTYLE ALL                                
SORTKEY (trip_date, vendor_name);           