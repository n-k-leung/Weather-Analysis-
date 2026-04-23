# Weather Analysis 

## Overview
This is a lab that builds and end-to-end pipeline using Open Metro weather data, Airflow, and dbt. A Preset dashboard is built to visualize the temperature trends, weather comfortability, abnormal weather, and rain and dry streak analysis. This analysis covers three cities: San Jose, San Francisco, and Cupertino.

### Overall System Diagram
The overall system architecture is illustrated below. The system follows a ELT-based design, where data is first ingested using an ETL pipeline and then transformed using dbt. Weather data is extracted from the Open-Meteo API using Apache Airflow, processed, and loaded into Snowflake raw tables. Dbt transformations are executed through Airflow to generate analytical models, which are stored in Snowflake analytics tables. Finally, a BI tool is used to visualize insights derived from the transformed data.

![Dashboard](/diagram/workflow_diagram.png)

## Airflow Dags
- WeatherData_ETL: extracts and loads 3 years of raw weather data
- BuildETL_dbt_weather: transforms data for analysis, scheduled to run 15 minutes after WeatherData_ETL


These dag files are found under Airflow/dags

## dbt Files
Our dbt files are found under Airflow/dbt

Specifically our sql files that handle the transformation and analysis of the weather data are the following files:

/analytics
- anomaly_detection.sql: analyzing anomaly weather days
- precipitation_analysis.sql: analyzing precipitation, rain streak, and dry streak
- trend_seasonality_analysis.sql: analyzing temperature trend
- wind_comfort_analysis.sql: analyzing comfortablility 

/transform
- weather_transform.sql: takes raw weather data and transforms it for analysis

## Dashboard
The Preset (Superset) dashboard is viewable through this link: https://2a501928.us1a.app.preset.io/superset/dashboard/p/V9lBLnQRdjD/. Because this is built with a free preset account, only users invited are able to view this dashboard and workspace. 

### Screenshot
![Dashboard](/dashboard/weather_analysis_preset_dashboard.png)

### Dashboard Filters
- City filter: dynamically updates Annual Average Temperature Trend (3 month moving average), Seasonal Wind Comfort Score, Annual Dry Vs Rainy Streak, and Weather Abnormalities by Season
- Season filter: dynamically updates Annual Average Temperature Trend (3 month moving average), Distribution of Rainfall By City, Annual Dry Vs Rainy Streak, and Average Abnormality Days by City

## Findings
- Most abnormal weather is exterme UV days
- It is most comfortable when therthe wind strength is calm and it is summer
- San Francisco experiences the most rain fall 
- There is a significant amount of dry spell streaks and it peaks during the summer months
- People often feel that it is cooler than it actually is
- Summer experiences the most abnormal weather days

- ## Authors
- Natalie Leung
- Kazi Samina Maraj Mumu

