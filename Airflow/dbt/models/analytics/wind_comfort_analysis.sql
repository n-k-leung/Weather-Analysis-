WITH base AS (
    SELECT * FROM {{ ref('weather_transform') }}
),

final AS (
    SELECT
        city,
        date,
        year,
        month,
        season,
        wind_speed_10m_max,
        comfort_score,

        CASE
            WHEN wind_speed_10m_max < 12 THEN 'Calm'
            WHEN wind_speed_10m_max < 29 THEN 'Breezy'
            WHEN wind_speed_10m_max < 50 THEN 'Windy'
            WHEN wind_speed_10m_max < 75 THEN 'Very Windy'
            ELSE 'Storm'
        END AS wind_strength,

        CASE
            WHEN comfort_score >= 80 THEN 'Very Comfortable'
            WHEN comfort_score >= 60 THEN 'Comfortable'
            WHEN comfort_score >= 40 THEN 'Moderate'
            WHEN comfort_score >= 20 THEN 'Uncomfortable'
            ELSE 'Very Uncomfortable'
        END AS comfort_category

    FROM base
)

SELECT *
FROM final
ORDER BY city, date