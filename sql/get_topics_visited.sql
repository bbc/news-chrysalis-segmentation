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
       ('magazine'),
       ('home')
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
  AND page_section2 NOT IN (SELECT page_section FROM vb_unhelpful_page_sections)
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
FROM vb_news_topic_activity
WHERE audience_id IN (SELECT DISTINCT audience_id FROM vb_too_few_visits)--keep usrs who are not cold starts
GROUP BY 1, 2
ORDER BY 1, 3
;
--remove sport as a topic
DELETE FROM vb_page_topics WHERE page_section = 'sport';

--remove any topic with less than 1000 visits
DROP TABLE IF EXISTS vb_section_usage;
CREATE TEMP TABLE vb_section_usage as
SELECT page_section, count(distinct audience_id) as users, sum(topic_count) as count
FROM vb_page_topics
GROUP BY 1
HAVING count < 1000
ORDER BY 3 desc;

DELETE FROM vb_page_topics WHERE page_section IN (SELECT page_section FROM vb_section_usage);

-- find the percentage of pagesfro each topic for each user
DROP TABLE IF EXISTS vb_page_topics_perc;
CREATE TABLE vb_page_topics_perc as
with total_count as (SELECT audience_id, sum(topic_count) as total_count FROM vb_page_topics GROUP BY 1)
SELECT a.*, a.topic_count::double precision / b.total_count::double precision as topic_perc, trunc(topic_perc, 3)
FROM vb_page_topics a
         JOIN total_count b on a.audience_id = b.audience_id
ORDER BY a.audience_id
;


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