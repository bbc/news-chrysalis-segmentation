BEGIN;
set search_path TO 'central_insights_sandbox';

DROP TABLE IF EXISTS vb_dates;
CREATE TABLE vb_dates
(
    min_date date,
    max_date date
);
INSERT INTO vb_dates
VALUES ('2022-01-17', '2022-04-17');
SELECT * FROM vb_dates;

-- remove any sections we won't be using such as local news
DROP TABLE IF EXISTS vb_page_sections;
CREATE TABLE vb_page_sections
(
    page_section varchar(4000)
);
INSERT INTO vb_page_sections
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

--remove any children's account
DROP TABLE IF EXISTS vb_adult_users;
CREATE TABLE vb_adult_users as
    SELECT bbc_hid3 FROM prez.profile_extension WHERE age_range NOT IN ('0-5', '6-10', '11-15');

--- get the user's activity
DROP TABLE IF EXISTS vb_news_topic_activity;
CREATE TABLE vb_news_topic_activity as
SELECT DISTINCT dt,
                visit_id,
                audience_id,
                page_name,
                REPLACE(page_section, '-', '_') as page_section2
FROM s3_audience.audience_activity
WHERE dt BETWEEN (SELECT REPLACE(min_date, '-', '') FROM vb_dates) AND (SELECT REPLACE(max_date, '-', '') FROM vb_dates)
  AND destination = 'PS_NEWS'
  AND is_signed_in = TRUE
  and is_personalisation_on = TRUE
  AND page_section2 NOT ILIKE 'name=%'
  AND page_section2 IN (SELECT page_section FROM vb_page_sections)
  AND audience_id IN (SELECT bbc_hid3 FROM vb_adult_users)
;

---Find users who've don't have more then X visits
DROP TABLE IF EXISTS vb_too_few_visits;
CREATE TEMP TABLE vb_too_few_visits as
SELECT DISTINCT audience_id,
                count(distinct dt || visit_id) as visits
FROM vb_news_topic_activity
GROUP BY 1
HAVING visits >= 3;


-- collect the number of topics people have viewed
DROP TABLE IF EXISTS vb_page_topics;
CREATE TEMP TABLE vb_page_topics as
SELECT audience_id,
       page_section2 as page_section,
       count(*)                  as topic_count
FROM vb_news_topic_activity
WHERE audience_id IN (SELECT DISTINCT audience_id FROM vb_too_few_visits)--keep users who are not cold starts
GROUP BY 1, 2
ORDER BY 1, 3
;

SELECT distinct page_section FROM vb_page_topics;

-- find the percentage of pages from each topic for each user
DROP TABLE IF EXISTS vb_page_topics_perc;
CREATE TABLE vb_page_topics_perc as
with total_count as (SELECT audience_id, sum(topic_count) as total_count FROM vb_page_topics GROUP BY 1)
SELECT a.*, a.topic_count::double precision / b.total_count::double precision as topic_perc, trunc(topic_perc, 3)
FROM vb_page_topics a
         JOIN total_count b on a.audience_id = b.audience_id
ORDER BY a.audience_id
;

SELECT count(distinct audience_id) FROM vb_page_topics_perc;--7,706,467

-- to read into python
SELECT audience_id, page_section, topic_perc
FROM central_insights_sandbox.vb_page_topics_perc
WHERE audience_id IN
      (SELECT DISTINCT audience_id FROM central_insights_sandbox.vb_page_topics_perc ORDER BY RANDOM() LIMIT 1000)
UNION
--this part is to ensure every topic is included
SELECT DISTINCT 'dummy'::varchar as audience_id, page_section, 0::double precision as topic_perc
FROM central_insights_sandbox.vb_page_topics_perc
ORDER BY 2;



GRANT ALL ON central_insights_sandbox.vb_page_topics_perc to edward_dearden with GRANT OPTION;

END;