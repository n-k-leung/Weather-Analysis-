WITH base AS (
    SELECT
        city,
        date,
        YEAR(date) AS year,
        MONTH(date) AS month,
        DAY(date) AS day,
        temp_max,
        temp_mean,
        temp_min,
        apparent_temp_max,
        apparent_temp_mean,
        apparent_temp_min,

        CASE
            WHEN MONTH(date) IN (12, 1, 2) THEN 'Winter'
            WHEN MONTH(date) IN (3, 4, 5)  THEN 'Spring'
            WHEN MONTH(date) IN (6, 7, 8)  THEN 'Summer'
            ELSE 'Fall'
        END AS season,
        weather_code,
        uv_index_max,
        uv_index_clear_sky_max,
        wind_speed_10m_max,
        precipitation_sum,
        rain_sum,
        showers_sum,
        precipitation_hours,
        precipitation_probability_max,
        precipitation_probability_mean,
        precipitation_probability_min,

        CASE WHEN precipitation_sum > 0 THEN 1 ELSE 0 END  AS is_rainy_day,
        CASE WHEN precipitation_sum >= 10 THEN 1 ELSE 0 END AS is_heavy_rain_day,

        GREATEST(0, LEAST(100,
            100
            - GREATEST(0, temp_mean - 25) * 3
            - GREATEST(0, 15 - temp_mean) * 3
            - wind_speed_10m_max * 0.5
            - CASE WHEN is_rainy_day = 1 THEN 10 ELSE 0 END
        )) AS comfort_score

    FROM {{source("raw", "weather_data_lab2")}}
    
    -- handles cases where these are null because it causes errors in dbt test
    WHERE temp_mean IS NOT NULL
        AND precipitation_sum IS NOT NULL
)

SELECT * FROM base