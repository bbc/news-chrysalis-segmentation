set search_path TO 'central_insights_sandbox';
DROP TABLE vb_app_visits;
CREATE TEMP TABLE vb_app_visits as
SELECT DISTINCT date_of_event,
                visit_id,
                CASE
                    WHEN app_name ILIKE '%chrysalis%' THEN 'chrysalis'
                    WHEN app_type = 'responsive' OR app_type = 'web' OR app_type = 'amp' THEN 'web'
                    WHEN app_type = 'mobile-app' THEN 'app'
                    ELSE app_name END as app_name_simple
FROM audience.audience_activity_daily_summary_enriched
WHERE destination = 'PS_NEWS'
  AND date_of_event = '2022-03-01'
  AND app_name_simple != 'web'
  AND app_name IS NOT NULL
ORDER BY random()
;
SELECT count(*) FROM vb_app_visits;--2,190,352
SELECT * FROM vb_app_visits ORDER BY random() LIMIT 3;

SELECT visit_id, event_position, placement, container, attribute, result,metadata, user_experience, publisher_clicks, publisher_impressions
FROM s3_audience.publisher
WHERE destination = 'PS_NEWS'
AND dt = '20220301'
AND visit_id in (SELECT DISTINCT visit_id FROM  vb_app_visits)
ORDER BY visit_id, event_position
LIMIT 100
;
