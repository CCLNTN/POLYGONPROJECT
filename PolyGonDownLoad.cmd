@echo off

REM Sync Daily Aggregate Data
aws s3 sync s3://flatfiles/us_stocks_sip/day_aggs_v1/2025/ Z:\2025\DAILY\GZFILES --endpoint-url https://files.polygon.io

REM Sync Trades Data
aws s3 sync s3://flatfiles/us_stocks_sip/trades_v1/2025/ Z:\2025\TRADES\GZFILES --endpoint-url https://files.polygon.io

REM Sync Quotes Data to the correct local folder
aws s3 sync s3://flatfiles/us_stocks_sip/quotes_v1/2025/ Z:\2025\QUOTES\GZFILES --endpoint-url https://files.polygon.io

echo Sync Complete!
