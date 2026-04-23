USE USER_DB_COYOTE; --replace with own database
USE WAREHOUSE COYOTE_WH; --replace with own warehouse
CREATE SCHEMA IF NOT EXISTS ADHOC;

-- historical data (60 days per city)
select * from raw.weather_data_hw5;
-- forecasted data (7 days prediction)
select * from analytics.weather_data_forecast_hw5;
-- union table (historical + forecasted)
select * from analytics.weather_data_union_hw5;