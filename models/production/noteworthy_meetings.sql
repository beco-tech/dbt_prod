{{ config(materialized='table') }}

WITH meetings_extracted_raw AS (
  SELECT
    TIMESTAMP_TRUNC(CAST(ts AS TIMESTAMP), DAY) AS meeting_day,
    id AS meeting_id,
    (
      SELECT JSON_EXTRACT_SCALAR(attr_obj, '$.value')
      FROM UNNEST(JSON_EXTRACT_ARRAY(attr)) AS attr_obj
      WHERE JSON_EXTRACT_SCALAR(attr_obj, '$.key') = 'companies'
    ) AS company_ids,
    (
      SELECT JSON_EXTRACT_SCALAR(attr_obj, '$.value')
      FROM UNNEST(JSON_EXTRACT_ARRAY(attr)) AS attr_obj
      WHERE JSON_EXTRACT_SCALAR(attr_obj, '$.key') = 'notes'
    ) AS notes,
    (
      SELECT JSON_EXTRACT_SCALAR(attr_obj, '$.value')
      FROM UNNEST(JSON_EXTRACT_ARRAY(attr)) AS attr_obj
      WHERE JSON_EXTRACT_SCALAR(attr_obj, '$.key') = 'fireflies_link'
    ) AS fireflies,
  FROM
    `tech-111022.airtable.historical_meetings`
    ),
meetings_extracted AS (
  SELECT *
  FROM meetings_extracted_raw
  WHERE company_ids IS NOT NULL 
)
SELECT *
FROM meetings_extracted