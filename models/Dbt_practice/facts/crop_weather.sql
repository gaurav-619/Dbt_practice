SELECT
  state,
  crop,
  crop_year,
  season,
  area,
  production,
  yield,
  annual_rainfall,
  fertilizer,
  pesticide,
  avg_temp,
  min_temp,
  max_temp,
  precipitation
FROM {{ ref('stg_crop_weather') }}