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

DROP TABLE IF EXISTS vb_unhelpful_page_sections;
CREATE TABLE vb_unhelpful_page_sections
(
    page_section varchar(4000)
);
INSERT INTO vb_unhelpful_page_sections
VALUES ('beds_bucks_and_herts'),
       ('berkshire'),
       ('birmingham_and_black_country'),
       ('bradford_and_west_yorkshire'),
       ('bristol'),
       ('cambridgeshire'),
       ('cornwall'),
       ('coventry_and_warwickshire'),
       ('cumbria'),
       ('derbyshire'),
       ('devon'),
       ('dorset'),
       ('essex'),
       ('gloucestershire'),
       ('hampshire'),
       ('hereford_and_worcester'),
       ('humberside'),
       ('kent'),
       ('lancashire'),
       ('leeds_and_west_yorkshire'),
       ('leicester'),
       ('lincolnshire'),
       ('manchester'),
       ('merseyside'),
       ('norfolk'),
       ('northamptonshire'),
       ('nottingham'),
       ('oxford'),
       ('shropshire'),
       ('somerset'),
       ('south_yorkshire'),
       ('stoke_and_staffordshire'),
       ('suffolk'),
       ('surrey'),
       ('sussex'),
       ('tayside_and_central'),
       ('tees'),
       ('tyne_and_wear'),
       ('wiltshire'),
       ('york_and_north_yorkshire'),
       ('guernsey'),
       ('isle_of_man'),
       ('jersey'),
       ('foyle_and_west'),
       ('edinburgh_east_and_fife'),
       ('glasgow_and_west'),
       ('highlands_and_islands'),
       ('north_east_orkney_and_shetland'),
       ('south_scotland'),
       ('northern_ireland'),
       ('mid_wales'),
       ('north_east_wales'),
       ('north_west_wales'),
       ('south_east_wales'),
       ('south_west_wales'),
       ('africa'),
       ('dachaigh'),
       ('election'),
       ('england'),
       ('features'),
       ('feeds'),
       ('front_page'),
       ('have_your_say'),
       ('in_pictures'),
       ('london'),
       ('london_and_south_east'),
       ('news'),
       ('northern_ireland_politics'),
       ('northern_ireland'),
       ('other'),
       ('scotland_business'),
       ('scotland'),
       ('scotland_politics'),
       ('video_and_audio'),
       ('wales_politics'),
       ('wales'),
       ('help'),
       ('explainers'),
       ('blogs'),
       ('get_inspired'),
       ('magazine')
;
SELECT page_section
FROM vb_unhelpful_page_sections ORDER BY RANDOM() LIMIT 10;


DROP TABLE IF EXISTS vb_page_topics;
CREATE TEMP TABLE vb_page_topics as
with get_pages as (
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
      AND page_section2 NOT IN (SELECT page_section FROM vb_unhelpful_page_sections)
)
SELECT audience_id,
       CASE
           WHEN page_section2 IN (
                                 'american_football',
                                 'athletics',
                                 'baseball',
                                 'basketball',
                                 'badminton',
                                 'bowls',
                                 'boxing',
                                 'commonwealth_games',
                                 'cricket',
                                 'cycling',
                                 'darts',
                                 'disability_sport',
                                 'equestrian',
                                 'formula1',
                                  'football',
                                 'gaelic_games',
                                 'golf',
                                 'gymnastics',
                                 'hockey',
                                 'horse_racing',
                                 'ice_hockey',
                                 'karate',
                                 'mixed_martial_arts',
                                 'modern_pentathlon',
                                 'motorsport',
                                 'netball',
                                 'olympics',
                                 'rowing',
                                 'rugby_league',
                                 'rugby_union',
                                 'skateboarding',
                                 'snooker',
                                 'squash',
                                 'sport',
                                 'swimming',
                                 'sailing',
                                 'tennis',
                                 'triathlon',
                                 'weightlifting',
                                 'winter_olympics',
                                 'winter_sports',
                                'wrestling'
               ) THEN 'sport'
           ELSE page_section2 END as page_section,
       count(*)                  as topic_count
FROM get_pages
GROUP BY 1, 2
ORDER BY 1, 3
;

SELECT count(distinct audience_id) FROM vb_page_topics;--12,721,739
SELECT * FROM vb_page_topics LIMIT 30;
SELECT DISTINCT page_section FROM vb_page_topics;

DROP TABLE IF EXISTS vb_cold_start_users;
CREATE TEMP TABLE vb_cold_start_users as
SELECT audience_id, sum(topic_count)  as total FROM vb_page_topics GROUP BY 1 HAVING total = 1;

SELECT count(*) FROM vb_cold_start_users;--2,202,717
DELETE FROM vb_page_topics WHERE audience_id IN (SELECT audience_id FROM vb_cold_start_users);

SELECT count(distinct audience_id) FROM vb_page_topics;--10,519,022

--remove any topic with less than 1000 visits
DROP TABLE IF EXISTS vb_section_usage;
CREATE TEMP TABLE vb_section_usage as
SELECT page_section, count(distinct audience_id) as users, sum(topic_count) as count
FROM vb_page_topics
GROUP BY 1
HAVING count < 1000
ORDER BY 3 desc;

SELECT * FROM vb_section_usage;

DELETE FROM vb_page_topics
WHERE page_section IN (SELECT page_section FROM vb_section_usage);

--checks
SELECT * FROM vb_page_topics ORDER BY audience_id, topic_count DESC LIMIT 100;
SELECT count(distinct audience_id) FROM vb_page_topics; --10,519,017

SELECT count(*) FROM vb_page_topics; --65,012,054


DROP TABLE IF EXISTS vb_page_topics_perc;
CREATE TABLE vb_page_topics_perc as
with total_count as (SELECT audience_id, sum(topic_count) as total_count FROM vb_page_topics GROUP BY 1)
SELECT a.*, a.topic_count::double precision / b.total_count::double precision as topic_perc, trunc(topic_perc, 3)
FROM vb_page_topics a
         JOIN total_count b on a.audience_id = b.audience_id
ORDER BY a.audience_id
;


--checks
SELECT DISTINCT 'dummy'::varchar as audience_id, page_section, 0::double precision as topic_perc
FROM central_insights_sandbox.vb_page_topics_perc
ORDER BY 2;


SELECT distinct page_section FROM vb_page_topics_perc LIMIT 10;--47
SELECT * FROM central_insights_sandbox.vb_page_topics_perc LIMIT 10;
SELECT count(*) FROM vb_page_topics_perc LIMIT 10;--70,976,890


-- to read into python
SELECT audience_id, page_section, topic_perc
FROM central_insights_sandbox.vb_page_topics_perc
WHERE audience_id IN
      (SELECT DISTINCT audience_id FROM central_insights_sandbox.vb_page_topics_perc ORDER BY RANDOM() LIMIT 1000)
UNION
SELECT DISTINCT 'dummy'::varchar as audience_id, page_section, 0::double precision as topic_perc
FROM central_insights_sandbox.vb_page_topics_perc
ORDER BY 2;



GRANT ALL ON central_insights_sandbox.vb_page_topics_perc to edward_dearden;
SELECT DISTINCT page_section
FROM central_insights_sandbox.vb_page_topics_perc
ORDER BY 1;


SELECT page_section, sum(topic_count) as topic_count, count(distinct audience_id) as users
FROM central_insights_sandbox.vb_page_topics_perc
GROUP BY 1
ORDER BY 3 DESC
;
