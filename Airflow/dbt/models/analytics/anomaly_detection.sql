WITH base AS (
    SELECT
        city,
        date,
        year,
        month,
        season,
        temp_max,
        temp_min,
        precipitation_sum,
        wind_speed_10m_max,
        uv_index_max
    FROM {{ ref('weather_transform') }}
),

thresholds AS (
    SELECT
        city,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY temp_max) AS p95_temp_max,
        PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY temp_min) AS p05_temp_min,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY precipitation_sum) AS p95_precip,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY wind_speed_10m_max) AS p95_wind,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY uv_index_max) AS p95_uv
    FROM base
    GROUP BY city
)

SELECT
    b.city,
    b.year,
    b.month,
    b.season,

    COUNT(*) AS total_days,
    SUM(CASE WHEN b.temp_max >= t.p95_temp_max THEN 1 ELSE 0 END) AS heat_wave_days,
    SUM(CASE WHEN b.temp_min <= t.p05_temp_min THEN 1 ELSE 0 END) AS cold_snap_days,
    SUM(CASE WHEN b.precipitation_sum >= t.p95_precip THEN 1 ELSE 0 END) AS heavy_rain_days,
    SUM(CASE WHEN b.wind_speed_10m_max >= t.p95_wind THEN 1 ELSE 0 END) AS high_wind_days,
    SUM(CASE WHEN b.uv_index_max >= t.p95_uv THEN 1 ELSE 0 END) AS extreme_uv_days,

    SUM(
        CASE WHEN
            b.temp_max >= t.p95_temp_max OR
            b.temp_min <= t.p05_temp_min OR
            b.precipitation_sum >= t.p95_precip OR
            b.wind_speed_10m_max >= t.p95_wind OR
            b.uv_index_max >= t.p95_uv
        THEN 1 ELSE 0 END
    ) AS anomaly_days,

    ROUND(
        SUM(
            CASE WHEN
                b.temp_max >= t.p95_temp_max OR
                b.temp_min <= t.p05_temp_min OR
                b.precipitation_sum >= t.p95_precip OR
                b.wind_speed_10m_max >= t.p95_wind OR
                b.uv_index_max >= t.p95_uv
            THEN 1 ELSE 0 END
        ) * 100.0 / COUNT(*),
        1
    ) AS anomaly_pct

FROM base b
LEFT JOIN thresholds t
    ON b.city = t.city
GROUP BY
    b.city, b.year, b.month, b.season
ORDER BY
    b.city, b.year, b.month