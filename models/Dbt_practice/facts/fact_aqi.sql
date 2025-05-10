SELECT
  state,
  timestamp,
  pm2_5,
  pm10,
  no2,
  co,
  o3,
  so2
FROM {{ ref('stg_live') }}