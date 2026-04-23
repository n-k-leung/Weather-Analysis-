WITH base AS (
    SELECT
        city,
        date,
        year,
        month,
        season,
        precipitation_sum,
        precipitation_probability_mean,
        CASE WHEN precipitation_sum > 0 THEN 1 ELSE 0 END AS is_rainy_day
    FROM {{ ref('weather_transform') }}
),

final AS (
    SELECT
        *,
        SUM(CASE WHEN is_rainy_day = 0 THEN 1 ELSE 0 END) OVER (
            PARTITION BY city ORDER BY date
        ) AS rainy_group,

        SUM(CASE WHEN is_rainy_day = 1 THEN 1 ELSE 0 END) OVER (
            PARTITION BY city ORDER BY date
        ) AS dry_group
    FROM base
)

SELECT
    city,
    date,
    year,
    month,
    season,
    precipitation_sum,
    precipitation_probability_mean,
    is_rainy_day,

    COUNT(*) OVER (
        PARTITION BY city, rainy_group
        ORDER BY date
    ) AS rainy_streak,

    COUNT(*) OVER (
        PARTITION BY city, dry_group
        ORDER BY date
    ) AS dry_streak

FROM final
ORDER BY city, date