BEGIN;
set search_path TO '<params.schema>';

-- DROP TABLE IF EXISTS ed_dates;
CREATE TEMP TABLE ed_dates (
    min_date date,
    max_date date
);

INSERT INTO ed_dates VALUES (cast('<params.date>'::varchar AS date)-(cast('<params.num_days>'::varchar AS int)), cast('<params.date>'::varchar AS date));
-- INSERT INTO ed_dates VALUES ('2022-01-17', '2022-02-14');
-- GRANT ALL ON ed_dates TO edward_dearden WITH GRANT OPTION;

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

DROP TABLE IF EXISTS <params.table_name>;
CREATE TABLE <params.table_name> as
with total_count as (SELECT audience_id, sum(topic_count) as total_count FROM ed_page_topics GROUP BY 1)
SELECT a.*, a.topic_count::double precision / b.total_count::double precision as topic_perc, trunc(topic_perc,3)
FROM ed_page_topics a
         JOIN total_count b on a.audience_id = b.audience_id
ORDER BY a.audience_id
;
GRANT ALL ON <params.table_name> TO edward_dearden WITH GRANT OPTION;
GRANT ALL ON <params.table_name> TO vicky_banks WITH GRANT OPTION;
GRANT ALL ON <params.table_name> TO GROUP central_insights WITH GRANT OPTION;

END