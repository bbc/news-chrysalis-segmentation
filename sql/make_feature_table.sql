BEGIN;
set search_path TO 'central_insights_sandbox';

DROP TABLE IF EXISTS ed_dates;
CREATE TABLE ed_dates (
    min_date date,
    max_date date
);

INSERT INTO ed_dates VALUES (cast('<params.date>'::varchar AS date)-(cast('<params.num_days>'::varchar AS int), cast('<params.date>'::varchar AS date))
-- INSERT INTO ed_dates VALUES ('2022-01-17', '2022-02-14')
;

CREATE TEMP TABLE ed_page_topics as
with get_pages as (
    SELECT DISTINCT dt, visit_id, audience_id, page_name, page_section
    FROM s3_audience.audience_activity
    WHERE dt BETWEEN (SELECT REPLACE(min_date, '-', '') FROM ed_dates) AND (SELECT REPLACE(max_date, '-', '') FROM ed_dates)
      AND destination = 'PS_NEWS'
      AND is_signed_in = TRUE
      and is_personalisation_on = TRUE
      AND page_section NOT ILIKE 'name=%'
)
SELECT audience_id, page_section, count(*) as topic_count
FROM get_pages
GROUP BY 1,2
ORDER BY 1,3
;

DROP TABLE IF EXISTS ed_current_data_to_segment;
CREATE TABLE ed_current_data_to_segment as
with total_count as (SELECT audience_id, sum(topic_count) as total_count FROM ed_page_topics GROUP BY 1)
SELECT a.*, a.topic_count::double precision / b.total_count::double precision as topic_perc, trunc(topic_perc,3)
FROM ed_page_topics a
         JOIN total_count b on a.audience_id = b.audience_id
ORDER BY a.audience_id
;

-- -- to read into python
-- SELECT audience_id, page_section, topic_perc
-- FROM central_insights_sandbox.ed_page_topics_perc
-- WHERE audience_id IN
--       (SELECT DISTINCT audience_id FROM central_insights_sandbox.ed_page_topics_perc ORDER BY RANDOM() LIMIT 1000)
-- UNION
-- SELECT DISTINCT 'dummy'::varchar as audience_id, page_section, 0::double precision as topic_perc FROM central_insights_sandbox.ed_page_topics_perc ORDER BY 2;

END