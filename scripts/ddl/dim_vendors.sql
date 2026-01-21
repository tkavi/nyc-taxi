CREATE TABLE IF NOT EXISTS dim_vendors (
    vendorid BIGINT ENCODE AZ64,
    vendor_name VARCHAR(100) ENCODE ZSTD,
    updated_by VARCHAR(50) ENCODE ZSTD,
    approved_by VARCHAR(50) ENCODE ZSTD, 
    effective_from TIMESTAMP ENCODE AZ64,
    effective_to TIMESTAMP ENCODE AZ64,
    is_current_flag BOOLEAN ENCODE RAW, 
    version INT DEFAULT 1 ENCODE AZ64
) DISTSTYLE ALL
SORTKEY (vendorid,is_current_flag);