set search_path TO 'central_insights_sandbox';

DROP TABLE IF EXISTS vb_dates;
CREATE TABLE vb_dates (
    min_date date,
    max_date date
);
INSERT INTO vb_dates VALUES ('2022-01-15', '2022-03-31');
SELECT * FROM vb_dates;

--Users per week
DROP TABLE IF EXISTS vb_news_basics;
CREATE TABLE vb_news_basics as
SELECT
       CASE
           WHEN app_name ILIKE '%chrysalis%' THEN 'chrysalis'
           WHEN app_type = 'responsive' OR app_type = 'web' OR app_type = 'amp' THEN 'web'
           WHEN app_type = 'mobile-app' THEN 'app'
           ELSE app_name END       as app_type,
       CASE
           WHEN gender = 'male' THEN 'male'
           WHEN gender = 'female' THEN 'female'
           ELSE 'unknown' END      as gender,
       CASE
           WHEN age_range IN ('16-19', '20-24') THEN '16-24'
           WHEN age_range IN ('25-29', '30-34') THEN '24-34'
           WHEN age_range IN ('35-39', '40-44') THEN '34-44'
           WHEN age_range IN ('45-49', '50-54') THEN '45-54'
           WHEN age_range IN ('55-59', '60-64', '65-70', '>70') THEN '55+'
           ELSE 'unknown' END      as age_range,
       CASE WHEN acorn_category ISNULL THEN 'unknown' ELSE
           LPAD(acorn_category::text, 2, '0') || '_' || acorn_category_description END as acorn_cat,
       count(distinct audience_id) as users
FROM audience.audience_activity_daily_summary_enriched
WHERE destination = 'PS_NEWS'
  AND date_of_event BETWEEN (SELECT min_date FROM vb_dates) AND (SELECT max_date FROM vb_dates)
  AND geo_country_site_visited = 'United Kingdom'
  AND is_personalisation_on = TRUE
  AND age_range NOT IN ('0-5', '6-10', '11-15')
AND app_type IS NOT NULL
GROUP BY 1, 2, 3, 4
;

SELECT distinct age_range FROM vb_news_basics ;