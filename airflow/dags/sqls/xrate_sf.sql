INSERT INTO exchange_rate_hist 
SELECT 
T1.$1:timestamp::TIMESTAMP_NTZ AS timestamp,
T1.$1:base::VARCHAR AS base_currency,
T2.$2 AS exchange_currency,
T2.$5 AS exchange_rate
FROM @MY_S3_STAGE/xrate.json (file_format => 'my_json_format') T1,
     lateral flatten(input => $1, path => 'rates') T2