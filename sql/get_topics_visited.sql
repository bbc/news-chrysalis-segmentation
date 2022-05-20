BEGIN;
set search_path TO 'central_insights_sandbox';

DROP TABLE IF EXISTS vb_dates;
CREATE TABLE vb_dates (
    min_date date,
    max_date date
);
INSERT INTO vb_dates VALUES ('2022-01-17', '2022-02-14');
SELECT * FROM vb_dates;

CREATE TEMP TABLE vb_page_topics as
with get_pages as (
    SELECT DISTINCT dt, visit_id, audience_id, page_name,
                    REPLACE(page_section,'-','_') as page_section
    FROM s3_audience.audience_activity
    WHERE dt BETWEEN (SELECT REPLACE(min_date, '-', '') FROM vb_dates) AND (SELECT REPLACE(max_date, '-', '') FROM vb_dates)
      AND destination = 'PS_NEWS'
      AND is_signed_in = TRUE
      and is_personalisation_on = TRUE
      AND page_section NOT ILIKE 'name=%'

)
SELECT audience_id,
       CASE
           WHEN page_section in ('beds_bucks_and_herts',
                                 'berkshire',
                                 'birmingham_and_black_country',
                                 'bradford_and_west_yorkshire',
                                 'bristol',
                                 'cambridgeshire',
                                 'cornwall',
                                 'coventry_and_warwickshire',
                                 'cumbria',
                                 'derbyshire',
                                 'devon',
                                 'dorset',
                                 'essex',
                                 'gloucestershire',
                                 'hampshire',
                                 'hereford_and_worcester',
                                 'humberside',
                                 'kent',
                                 'lancashire',
                                 'leeds_and_west_yorkshire',
                                 'leicester',
                                 'lincolnshire',
                                 'manchester',
                                 'merseyside',
                                 'norfolk',
                                 'northamptonshire',
                                 'nottingham',
                                 'oxford',
                                 'shropshire',
                                 'somerset',
                                 'south_yorkshire',
                                 'stoke_and_staffordshire',
                                 'suffolk',
                                 'surrey',
                                 'sussex',
                                 'tayside_and_central',
                                 'tees',
                                 'tyne_and_wear',
                                 'wiltshire',
                                 'york_and_north_yorkshire') THEN 'region_england'
           WHEN page_section IN ('guernsey',
                                 'isle_of_man',
                                 'jersey') THEN 'region_islands'
           WHEN page_section IN ('foyle_and_west') THEN 'region_northern_ireland'
           WHEN page_section IN ('edinburgh_east_and_fife',
                                 'glasgow_and_west',
                                 'highlands_and_islands',
                                 'north_east_orkney_and_shetland',
                                 'south_scotland') THEN 'region_scotland'
           WHEN page_section IN ('mid_wales',
                                 'north_east_wales',
                                 'north_west_wales',
                                 'south_east_wales',
                                 'south_west_wales') THEN 'region_wales'
           ELSE page_section END as page_section,

       count(*)                  as topic_count
FROM get_pages
GROUP BY 1, 2
ORDER BY 1, 3
;
--remove any topic with less than 100 visits
CREATE TEMP TABLE vb_section_usage as
SELECT page_section, count(distinct audience_id) as users, sum(topic_count) as count  FROM vb_page_topics GROUP BY 1
HAVING count <1000
ORDER BY 3 desc;

DELETE FROM vb_page_topics WHERE page_section IN (SELECT page_section FROM vb_section_usage );

--checks
SELECT * FROM vb_page_topics ORDER BY audience_id, topic_count DESC LIMIT 10;
SELECT count(distinct audience_id) FROM vb_page_topics  LIMIT 10; --9,275,293
SELECT count(*) FROM vb_page_topics  LIMIT 10; --82,048,718



DROP TABLE IF EXISTS vb_page_topics_perc;
CREATE TABLE vb_page_topics_perc as
with total_count as (SELECT audience_id, sum(topic_count) as total_count FROM vb_page_topics GROUP BY 1)
SELECT a.*, a.topic_count::double precision / b.total_count::double precision as topic_perc, trunc(topic_perc,3)
FROM vb_page_topics a
         JOIN total_count b on a.audience_id = b.audience_id
ORDER BY a.audience_id
;


--checks
SELECT DISTINCT 'dummy'::varchar as audience_id, page_section, 0::double precision as topic_perc FROM central_insights_sandbox.vb_page_topics_perc ORDER BY 2;
<<<<<<< Updated upstream
SELECT count(distinct page_section) FROM vb_page_topics_perc    LIMIT 10;--67
SELECT * FROM central_insights_sandbox.vb_page_topics_perc    LIMIT 10;
SELECT count(*) FROM vb_page_topics_perc    LIMIT 10;--82,048,718
=======
SELECT distinct page_section FROM vb_page_topics_perc    LIMIT 10;--47
SELECT * FROM central_insights_sandbox.vb_page_topics_perc    LIMIT 10;
SELECT count(*) FROM vb_page_topics_perc    LIMIT 10;--78,325,452

>>>>>>> Stashed changes


-- to read into python
SELECT audience_id, page_section, topic_perc
FROM central_insights_sandbox.vb_page_topics_perc
WHERE audience_id IN
      (SELECT DISTINCT audience_id FROM central_insights_sandbox.vb_page_topics_perc ORDER BY RANDOM() LIMIT 1000)
UNION
SELECT DISTINCT 'dummy'::varchar as audience_id, page_section, 0::double precision as topic_perc FROM central_insights_sandbox.vb_page_topics_perc ORDER BY 2;
<<<<<<< Updated upstream


GRANT ALL ON central_insights_sandbox.vb_page_topics_perc to edward_dearden;
SELECT DISTINCT  page_section FROM central_insights_sandbox.vb_page_topics_perc ORDER BY 1;




=======


GRANT ALL ON central_insights_sandbox.vb_page_topics_perc to edward_dearden;
SELECT DISTINCT  page_section FROM central_insights_sandbox.vb_page_topics_perc ORDER BY 1;

SELECT page_section, sum(topic_count) as topic_count, count(distinct audience_id) as users
FROM central_insights_sandbox.vb_page_topics_perc
GROUP BY 1
ORDER BY 3 DESC
;
>>>>>>> Stashed changes



