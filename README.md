# Weather Analysis 

## Overview
This is a lab that builds and end-to-end pipeline using Open Metro weather data, Airflow, and dbt. A Preset dashboard is built to visualize the temperature trends, weather comfortability, abnormal weather, and rain and dry streak analysis. This analysis covers three cities: San Jose, San Francisco, and Cupertino.

## Airflow Dags
- WeatherData_ETL: extracts and loads 3 years of raw weather data
- BuildETL_dbt_weather: transforms data for analysis

## Dashboard
The Preset (Superset) dashboard is accessible through this link: https://2a501928.us1a.app.preset.io/superset/dashboard/9/?native_filters_key=Wl0UusbnAKY 

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
- Natalie Leung (natalie.leung@sjsu.edu)
- Kazi Samina Maraj Mumu (kazisaminamaraj.mumu@sjsu.edu)

