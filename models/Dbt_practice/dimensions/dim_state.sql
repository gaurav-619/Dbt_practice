SELECT DISTINCT
  state
FROM (
  SELECT state FROM {{ ref('stg_crop_weather') }}
  UNION DISTINCT
  SELECT state FROM {{ ref('stg_live') }}
)
