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


-- Limit to sections we want
CREATE TEMP TABLE ed_page_sections
(
    page_section varchar(4000)
);
INSERT INTO ed_page_sections
VALUES ('business'),
       ('disability'),
       ('education'),
       ('entertainment_and_arts'),
       ('health'),
       ('newsbeat'),
       ('politics'),
       ('reality_check'),
       ('science_and_environment'),
       ('stories'),
       ('technology'),
       ('uk'),
       ('world')
;

-- create a list of adults
CREATE TEMP TABLE ed_adult_users as
SELECT bbc_hid3 FROM prez.profile_extension WHERE age_range NOT IN ('0-5', '6-10', '11-15');


--- get the user's activity
CREATE TEMP TABLE ed_news_topic_activity as
SELECT DISTINCT dt,
                visit_id,
                audience_id,
                page_name,
                REPLACE(page_section, '-', '_') as page_section2
FROM s3_audience.audience_activity
WHERE dt BETWEEN (SELECT REPLACE(min_date, '-', '') FROM ed_dates) AND (SELECT REPLACE(max_date, '-', '') FROM ed_dates)
  AND destination = 'PS_NEWS'
  AND is_signed_in = TRUE
  and is_personalisation_on = TRUE
  AND page_section2 NOT ILIKE 'name=%'
  AND page_section2 IN (SELECT page_section FROM ed_page_sections)
  AND audience_id IN (SELECT bbc_hid3 FROM ed_adult_users)
;

---Find users who've don't have more then X visits
CREATE TEMP TABLE ed_too_few_visits as
SELECT DISTINCT audience_id,
                count(distinct dt || visit_id) as visits
FROM ed_news_topic_activity
GROUP BY 1
HAVING visits >= <params.minimum_visits>;


-- collect the number of topics people have viewed
CREATE TEMP TABLE ed_page_topics as
SELECT audience_id,
       page_section2 as page_section,
       count(*) as topic_count
FROM ed_news_topic_activity
WHERE audience_id IN (SELECT DISTINCT audience_id FROM ed_too_few_visits)--keep users who are not cold starts
GROUP BY 1, 2
ORDER BY 1, 3
;

DROP TABLE IF EXISTS <params.table_name>;
CREATE TABLE <params.table_name> as
  with total_count as (
      SELECT audience_id, sum(topic_count) as total_count
        FROM ed_page_topics
        GROUP BY 1
    )
    SELECT a.*, a.topic_count::double precision / b.total_count::double precision as topic_perc, trunc(topic_perc,3)
      FROM ed_page_topics a
      JOIN total_count b on a.audience_id = b.audience_id
      ORDER BY a.audience_id
  ;

GRANT ALL ON <params.table_name> TO edward_dearden WITH GRANT OPTION;
GRANT ALL ON <params.table_name> TO vicky_banks WITH GRANT OPTION;
GRANT ALL ON <params.table_name> TO GROUP central_insights;

CREATE TABLE IF NOT EXISTS taste_segmentation_training_meta (
    start_date date,
    end_date date,
    num_users int
);
INSERT INTO taste_segmentation_training_meta
SELECT
  cast('<params.date>'::varchar AS date)-(cast('<params.num_days>'::varchar AS int)),
  cast('<params.date>'::varchar AS date),
  COUNT(DISTINCT audience_id)
FROM <params.table_name>;

GRANT ALL ON taste_segmentation_training_meta TO edward_dearden WITH GRANT OPTION;
GRANT ALL ON taste_segmentation_training_meta TO vicky_banks WITH GRANT OPTION;
GRANT ALL ON taste_segmentation_training_meta TO GROUP central_insights;

END;
