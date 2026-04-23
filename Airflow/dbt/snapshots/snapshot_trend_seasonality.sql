{% snapshot snapshot_trend_seasonality %}
{{
    config(
        target_schema='snapshot',
        unique_key="city || '-' || CAST(year AS VARCHAR) || '-' || LPAD(CAST(month AS VARCHAR), 2, '0')",
        strategy='timestamp',
        updated_at='month_date',
        invalidate_hard_deletes=True
    )
}}
SELECT
    *,
    DATE_FROM_PARTS(year, month, 1) AS month_date
FROM {{ ref('trend_seasonality_analysis') }}
{% endsnapshot %}
