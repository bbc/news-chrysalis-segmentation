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
        SELECT DISTINCT dt, visit_id, audience_id, page_name,
                        REPLACE(page_section,'-','_') as page_section
        FROM s3_audience.audience_activity
        WHERE dt BETWEEN (SELECT REPLACE(min_date, '-', '') FROM ed_dates) AND (SELECT REPLACE(max_date, '-', '') FROM ed_dates)
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

CREATE TEMP TABLE ed_section_usage as
  SELECT page_section, count(distinct audience_id) as users, sum(topic_count) as count
    FROM ed_page_topics
    GROUP BY 1
    HAVING count <1000
    ORDER BY 3 desc;

DELETE FROM ed_page_topics WHERE page_section IN (SELECT page_section FROM ed_section_usage);

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

END;
