set search_path TO 'central_insights_sandbox';

DROP TABLE IF EXISTS vb_dates;
CREATE TABLE vb_dates (
    min_date date,
    max_date date
);
INSERT INTO vb_dates VALUES ('2022-01-15', '2022-03-31');
SELECT * FROM vb_dates;

--Users per week
CREATE TABLE vb_news_basics as
SELECT
       date_of_event,
       CASE
           WHEN app_name ILIKE '%chrysalis%' THEN 'mobile-chrysalis'
           WHEN app_type = 'responsive' OR app_type = 'web' OR app_type = 'amp' THEN 'web'
           WHEN app_type = 'mobile-app' THEN 'app'
           ELSE app_name END       as app_type,
       gender, age_range, acorn_category,
       count(distinct audience_id) as users
FROM audience.audience_activity_daily_summary_enriched
WHERE destination = 'PS_NEWS'
  AND date_of_event BETWEEN (SELECT min_date FROM vb_dates) AND (SELECT max_date FROM vb_dates)
  AND geo_country_site_visited = 'United Kingdom'
  AND is_personalisation_on = TRUE
GROUP BY 1,2,3,4,5
;
DELETE FROM vb_news_basics WHERE app_type ISNULL;
SELECT * FROM vb_news_basics LIMIT 10;