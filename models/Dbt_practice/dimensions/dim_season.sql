SELECT DISTINCT
  season
FROM {{ ref('stg_crop_weather') }}
