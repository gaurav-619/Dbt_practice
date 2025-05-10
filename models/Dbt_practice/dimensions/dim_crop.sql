SELECT DISTINCT
  crop
FROM {{ ref('stg_crop_weather') }}
