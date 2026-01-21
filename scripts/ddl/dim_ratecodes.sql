CREATE TABLE dim_ratecodes (
    ratecodeid INT ENCODE AZ64,
    ratecode_desc VARCHAR(50) ENCODE ZSTD,
    updated_by VARCHAR(50) ENCODE ZSTD,
    approved_by VARCHAR(50) ENCODE ZSTD, 
    effective_from TIMESTAMP ENCODE AZ64,
    effective_to TIMESTAMP ENCODE AZ64,
    is_current_flag BOOLEAN ENCODE RAW, 
    version INT DEFAULT 1 ENCODE AZ64
) DISTSTYLE ALL
SORTKEY (ratecodeid,is_current_flag);