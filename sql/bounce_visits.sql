set search_path TO 'central_insights_sandbox';
/*
 - get all the pages in the visit
 - find all the pages with one visit
 - what proportion are to homepage only?
 - then of those only on homepage, how many on chrysalis use the scroll?
 */

DROP TABLE IF EXISTS vb_dates;
CREATE TABLE vb_dates (
    min_date date,
    max_date date
);
INSERT INTO vb_dates VALUES ('2022-01-15', '2022-03-31');
SELECT * FROM vb_dates;

-- get the visits
DROP TABLE IF EXISTS vb_pages_per_visit;
CREATE TABLE vb_pages_per_visit as
with get_subsections as (
    --- get the cleaned page name and the page section i.e World/politics
    SELECT distinct REVERSE(SPLIT_PART(REVERSE(page_name), '::', 1)) AS page_name_cleaned,
                    page_section
    FROM s3_audience.audience_activity
    WHERE destination = 'PS_NEWS'
      AND dt BETWEEN (SELECT REPLACE(min_date, '-', '') FROM vb_dates) AND (SELECT REPLACE(max_date, '-', '') FROM vb_dates)
    AND page_section IS NOT NULL
    GROUP BY 1, 2

),
     users as (
         -- get users demographics and the pages they visit
         SELECT DISTINCT date_of_event::date                                                                  as dt,
                         audience_id,
                         visit_id,
                         date_of_event || '-' || visit_id                                                     as dist_visit,
                         CASE
                             WHEN app_name ILIKE '%chrysalis%' THEN 'chrysalis'
                             WHEN app_type = 'responsive' OR app_type = 'web' OR app_type = 'amp' THEN 'web'
                             WHEN app_type = 'mobile-app' THEN 'app'
                             ELSE app_name END                                                                as app_type,
                         CASE
                             WHEN gender = 'male' THEN 'male'
                             WHEN gender = 'female' THEN 'female'
                             ELSE 'unknown' END                                                               as gender,
                         CASE
                             WHEN age_range IN ('16-19', '20-24') THEN '16-24'
                             WHEN age_range IN ('25-29', '30-34') THEN '24-34'
                             WHEN age_range IN ('35-39', '40-44') THEN '34-44'
                             WHEN age_range IN ('45-49', '50-54') THEN '45-54'
                             WHEN age_range IN ('55-59', '60-64', '65-70', '>70') THEN '55+'
                             ELSE 'unknown' END                                                               as age_range,
                         CASE
                             WHEN acorn_category ISNULL THEN 'unknown'
                             ELSE
                                 LPAD(acorn_category::text, 2, '0') || '_' || acorn_category_description END  as acorn_cat,
                         page_name,
                         REVERSE(SPLIT_PART(REVERSE(page_name), '::', 1))                                     AS page_name_cleaned
         FROM audience.audience_activity_daily_summary_enriched
         WHERE destination = 'PS_NEWS'
           AND date_of_event BETWEEN (SELECT min_date FROM vb_dates) AND (SELECT max_date FROM vb_dates)
           AND geo_country_site_visited = 'United Kingdom'
           AND is_personalisation_on = TRUE
           AND age_range NOT IN ('0-5', '6-10', '11-15')
           AND app_type IS NOT NULL
         ORDER BY 1, 2, 3
     )

SELECT a.*, b.page_section
FROM users a
         LEFT JOIN get_subsections b on a.page_name_cleaned = b.page_name_cleaned
;

SELECT * FROM vb_pages_per_visit LIMIT 100;
SELECT DISTINCT page_name_cleaned, count(*) FROM vb_pages_per_visit GROUP BY 1 ORDER BY 2 DESC;
SELECT app_type, page_name_cleaned,count(*) FROM vb_pages_per_visit WHERE page_section ISNULL GROUP BY 1,2 ORDER BY 3 DESC;

SELECT count(*) as rows, count(distinct dt||visit_id) as visits, count(distinct audience_id) as users FROM vb_pages_per_visit;

--identify the bounce visits
DROP TABLE IF EXISTS vb_bounce_visit_ids;
CREATE TABLE vb_bounce_visit_ids AS
SELECT distinct dt, audience_id, visit_id, count(distinct page_name) as num_pages, dt||'-'|| visit_id as dist_visit
FROM vb_pages_per_visit
GROUP BY 1,2,3
HAVING num_pages =1
ORDER BY 1,2,3;

SELECT count(*) as rows, count(distinct dt||visit_id) as visits, count(distinct audience_id) as users FROM vb_bounce_visit_ids;

-- select the bounce visits
DROP TABLE IF EXISTS vb_bounce_visits;
CREATE TABLE vb_bounce_visits as
SELECT *
FROM vb_pages_per_visit
WHERE dist_visit IN (SELECT dist_visit FROM vb_bounce_visit_ids);

SELECT * FROM vb_bounce_visits ORDER BY dist_visit ;
SELECT count(*) as rows, count(distinct dt||visit_id) as visits, count(distinct audience_id) as users FROM vb_bounce_visits;

-- For the top 10 bounce pages, what % of bounce visits are on that page?
--CREATE TABLE vb_bounce_summary as
INSERT INTO vb_bounce_summary
with total_visits as (SELECT dt, app_type, count(dist_visit) as total_visits FROM vb_bounce_visits GROUP BY 1,2),
     page_visits as (
         SELECT dt, app_type,
                page_name_cleaned,
                count(distinct dist_visit)                                     as visits,
                row_number() over (partition by app_type order by visits DESC) as rank
         FROM vb_bounce_visits
         GROUP BY 1, 2,3
         )
SELECT a.*, round(100*a.visits::double precision/b.total_visits::double precision,1) as perc
FROM page_visits a
         LEFT JOIN total_visits b on a.app_type = b.app_type AND a.dt = b.dt
WHERE rank <=5;


--DROP TABLE IF EXISTS vb_homepage_bounce;
--CREATE TABLE vb_homepage_bounce as
  INSERT INTO  vb_homepage_bounce
    SELECT * FROM vb_bounce_visits WHERE page_name_cleaned IN ('news.page','news.discovery.page');

SELECT dt, count(*) FROM vb_homepage_bounce GROUP BY 1 ORDER BY 1;--1,636,221
SELECt * FROM vb_homepage_bounce LIMIT 10;
SELECT * FROM vb_bounce_visits LIMIT 100;


--- What % of bounce visits per day are on homepage?
SELECT *
FROM vb_bounce_summary
--WHERE page_name_cleaned IN ('news.page','news.discovery.page')
LIMIT 10;


-- total bounces
SELECt dt, app_type, count(dist_visit) as visits
FROM vb_bounce_visits
GROUP BY 1,2
ORDER BY 1,2
LIMIT 10;