CREATE TABLE IF NOT EXISTS dim_zones (
    locationid INT ENCODE AZ64,
    borough VARCHAR(50) ENCODE ZSTD,
    zone VARCHAR(100) ENCODE ZSTD,       
    service_zone VARCHAR(50) ENCODE ZSTD,
    updated_by VARCHAR(50) ENCODE ZSTD,
    approved_by VARCHAR(50) ENCODE ZSTD, 
    effective_from TIMESTAMP ENCODE AZ64,
    effective_to TIMESTAMP ENCODE AZ64,
    is_current_flag BOOLEAN ENCODE RAW, 
    version INT DEFAULT 1 ENCODE AZ64
) 
DISTSTYLE ALL 
SORTKEY (locationid, is_current_flag); 