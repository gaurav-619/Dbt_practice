WITH latest_aqi AS (
  SELECT AS STRUCT *
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY state ORDER BY timestamp DESC) AS rn
    FROM {{ ref('fact_aqi') }}
  )
  WHERE rn = 1
)
SELECT
  cw.*,
  aqi.pm2_5,
  aqi.pm10,
  aqi.no2,
  aqi.co,
  aqi.o3,
  aqi.so2
FROM {{ ref('fact_crop_weather') }} cw
LEFT JOIN latest_aqi aqi ON cw.state = aqi.state
