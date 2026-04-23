WITH base AS (
    SELECT * FROM {{ ref('weather_transform') }}
),

monthly_avg AS (
    SELECT
        city,
        year,
        month,
        season,
        ROUND(AVG(temp_mean), 2)          AS avg_temp,
        ROUND(AVG(apparent_temp_mean), 2) AS avg_apparent_temp
    FROM base
    GROUP BY city, year, month, season
),

final AS (
    SELECT
        city,
        year,
        month,
        season,
        avg_temp,
        avg_apparent_temp,

        ROUND(AVG(avg_temp) OVER (
            PARTITION BY city
            ORDER BY year, month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 2) AS ma_3m_temp,

        ROUND(AVG(avg_apparent_temp) OVER (
            PARTITION BY city
            ORDER BY year, month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 2) AS ma_3m_apparent_temp

    FROM monthly_avg
)

SELECT
    city,
    year,
    month,
    season,
    avg_temp,
    avg_apparent_temp,
    ma_3m_temp,
    ma_3m_apparent_temp
FROM final
ORDER BY year, month