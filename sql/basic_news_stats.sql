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

--- visits & audience IDs daily
DROP TABLE IF EXISTS vb_news_daily;
CREATE TABLE vb_news_daily as
SELECT date_of_event,
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
       count(distinct audience_id) as users,
       count(distinct visit_id) as visits
FROM audience.audience_activity_daily_summary_enriched
WHERE destination = 'PS_NEWS'
  AND date_of_event BETWEEN (SELECT min_date FROM vb_dates) AND (SELECT max_date FROM vb_dates)
  AND geo_country_site_visited = 'United Kingdom'
  AND is_personalisation_on = TRUE
  AND age_range NOT IN ('0-5', '6-10', '11-15')
AND app_type IS NOT NULL
GROUP BY 1, 2, 3, 4,5
;
SELECT count(*) FROM vb_news_daily;--23710

SELECT distinct date_of_event FROM vb_news_daily ;

--- pages visited
CREATE TABLE vb_news_pages as
SELECT distinct
    date_of_event,
       audience_id,
                visit_id,
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
       count(distinct page_name) as pages
FROM audience.audience_activity_daily_summary_enriched
WHERE destination = 'PS_NEWS'
  AND date_of_event BETWEEN (SELECT min_date FROM vb_dates) AND (SELECT max_date FROM vb_dates)
  AND geo_country_site_visited = 'United Kingdom'
  AND is_personalisation_on = TRUE
  AND age_range NOT IN ('0-5', '6-10', '11-15')
AND app_type IS NOT NULL
GROUP BY 1, 2, 3, 4,5,6,7

;

CREATE TABLE vb_news_pages_summary as
SELECT date_of_event, app_type, gender, age_range, acorn_cat, pages, count(audience_id) as users, count(visit_id) as visits
FROM vb_news_pages
GROUP BY 1,2,3,4,5,6
;

SELECT app_type, CASE WHEN pages <= 5 then pages ELSE 5 END as pages,
       sum(visits) as visits
FROM vb_news_pages_summary
GROUP BY 1,2
;

SELECT *
FROM vb_news_pages
LIMIT 10;


SELECT app_type, median(pages), avg(pages), avg(pages::double precision)
FROM vb_news_pages
WHERE date_of_event = '2022-03-02'
GROUP BY 1
LIMIT 10;

------------ Do users who bounce on Chrysalis use the scroll? ---------------

-- get one page visits on Chrysalis
DROP TABLE IF EXISTS vb_bounce_visits;
CREATE TABLE vb_bounce_visits as
SELECT date_of_event::date as dt,
       visit_id,
       app_type
FROM vb_news_pages
WHERE pages = 1 AND app_type = 'chrysalis'
;
SELECT count(*) FROM vb_bounce_visits LIMIT 100;

CREATE TABLE vb_dates_bounce as SELECT DISTINCT dt FROM vb_bounce_visits;
SELECT * FROM vb_dates_bounce ORDER BY 1;

-- get their cookies to use in publisher
DROP TABLE IF EXISTS vb_users_chrys;
CREATE TABLE vb_users_chrys AS
with visits as (
    SELECT DISTINCT dt::date, unique_visitor_cookie_id, visit_id, mobile_device_manufacturer
    FROM s3_audience.visits
    WHERE dt = REPLACE('2022-01-15', '-', '')
      AND destination = 'PS_NEWS'
      AND is_signed_in = TRUE
      AND is_personalisation_on = TRUE
)
SELECT a.*, unique_visitor_cookie_id, mobile_device_manufacturer
FROM vb_bounce_visits a
         JOIN visits b on a.dt = b.dt AND a.visit_id = b.visit_id
;

SELECT * FROM vb_users_chrys LIMIT 10;
SELECT count(DISTINCT dt||visit_id) AS visits, count(DISTINCT unique_visitor_cookie_id) as cookies FROM vb_users_chrys ;
SELECT DISTINCT dt FROM vb_users_chrys;

-- did they scroll on the top stories carousel?
DROP TABLE IF EXISTS pub_data;
CREATE TABLE pub_data AS
SELECT DISTINCT dt::date,
                unique_visitor_cookie_id,
                visit_id,
                attribute
FROM s3_audience.publisher
WHERE dt = REPLACE('2022-01-15', '-', '')
  AND destination = 'PS_NEWS'
  AND unique_visitor_cookie_id IN (SELECT DISTINCT unique_visitor_cookie_id FROM vb_users_chrys)
  AND placement = 'news.discovery.page'
  AND attribute = 'top-stories~carousel-scroll-start'
  AND publisher_clicks = 1
;

DROP TABLE IF EXISTS vb_carousel_usage;
CREATE TABLE vb_carousel_usage as
INSERT INTO vb_carousel_usage
SELECT a.dt,a.visit_id, a.app_type, b.unique_visitor_cookie_id, b.attribute, mobile_device_manufacturer
FROM vb_users_chrys a
LEFT JOIN pub_data b on a.dt = b.dt AND a.visit_id = b.visit_id
;
DELETE FROM vb_carousel_usage;

SELECT dt, count(*) as rows, count(distinct dt||visit_id) as visits FROM vb_carousel_usage gROUP BY 1 ORDER BY 1;
SELECT * FROM vb_carousel_usage;

-- attribute = 'top-stories~carousel-scroll-reached-end'
-- attribute = 'top-stories~carousel-scroll-start'

SELECT * FROM central_insights_sandbox.vb_users_chrys LIMIT 100;
SELECT count(*) FROM central_insights_sandbox.vb_users_chrys;

SELECT * FROM central_insights_sandbox.pub_data LIMIT 100;
SELECT count(*) FROM central_insights_sandbox.pub_data;

