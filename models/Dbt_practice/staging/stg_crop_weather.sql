SELECT
  crop,
  crop_Year,
  season,
  state,
  area,
  production,
  annual_rainfall,
  fertilizer,
  pesticide,
  yield,
  avg_temp,
  min_temp,
  max_temp,
  precipitation
FROM `dbt-crop.crop_weather_ds.crop_weather`
