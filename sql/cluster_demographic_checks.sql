set search_path TO 'central_insights_sandbox';

alter table vb_chrys_feat_test
    add old_cluster int;

SELECT * FROM vb_chrys_feat_test LIMIT 10;
UPDATE vb_chrys_feat_test
set old_cluster = CASE
                      WHEN cluster = 0 THEN 6
                      WHEN cluster = 1 THEN 5
                      WHEN cluster = 2 THEN 4
                      WHEN cluster = 3 THEN 3
                      WHEN cluster = 4 THEN 1
                      WHEN cluster = 5 THEN 0
                      WHEN cluster = 6 THEN 2 END;

SELECT old_cluster, count(*) as users,(SELECT count(*)::int FROM vb_chrys_feat_test) as total,
       round(100*users::double precision/ total::double precision,0) as perc
FROM vb_chrys_feat_test GROUP BY 1 ORDER BY 1;

UPDATE vb_chrys_feat_test
set cluster = old_cluster;

SELECT cluster, count(*) as users,(SELECT count(*)::int FROM vb_chrys_feat_test) as total,
       round(100*users::double precision/ total::double precision,0) as perc
FROM vb_chrys_feat_test GROUP BY 1 ORDER BY 1;

ALTER TABLE vb_chrys_feat_test DROP old_cluster;

DROP TABLE IF EXISTS vb_cluster_demo;
CREATE TABLE vb_cluster_demo as
with get_data as (
    SELECT a.*,
           CASE
               WHEN b.age_range in ('16-19', '20-24') THEN '16-24'
               WHEN b.age_range in ('25-29', '30-34') THEN '25-34'
               WHEN b.age_range in ('35-39', '40-44') THEN '35-44'
               WHEN b.age_range in ('45-49', '50-54') THEN '45-54'
               WHEN b.age_range ISNULL THEN 'unknown'
               ELSE '55+' END                                  as age_range,
           CASE
               WHEN gender = 'male' THEN 'male'
               WHEN gender = 'female' THEN 'female'
               ELSE 'unknown' END                              as gender
            ,
           acorn_category || '-' || acorn_category_description as acorn_category,
           nation,
           segvalue
    FROM vb_chrys_feat_test a
             LEFT JOIN prez.profile_extension b on a.audience_id = b.bbc_hid3
    LEFT JOIN central_insights.segserver_SG10122_geo_proxy_1 c on a.audience_id = c.hashedid
)
SELECT cluster, age_range, gender, acorn_category, nation,segvalue, count(audience_id) as users
FROM get_data
GROUP BY 1, 2, 3, 4, 5,6
;

SELECT cluster, gender, sum(users) as users
FROM vb_cluster_demo
WHERE gender !='unknown'
GROUP BY 1, 2
ORDER BY 1, 2;

SELECT * FROM vb_cluster_demo ORDER BY cluster, nation, age_range, gender, acorn_category LIMIT 10;

SELECT sum(users)
FROM vb_cluster_demo

LIMIT 10;

---Join with audiences map
SELECT * FROM central_insights.segserver_SG10122_geo_proxy_1 LIMIT 10;

SELECT * FROM central_insights.SG10122_geo_proxy_1_definitions;

/*SELECT product,date_of_segmentation, bbc_hid3, segvalue
FROM central_insights.sg10026_info_individual_alltime
WHERE product = 'news'
AND date_of_segmentation BETWEEN (SELECT min_date FROM central_insights_sandbox.vb_dates) AND (SELECT max_date FROM central_insights_sandbox.vb_dates)
AND bbc_hid3 IN (SELECT DISTINCT audience_id FROM vb_chrys_feat_test)
ORDER BY bbc_hid3, date_of_segmentation
LIMIT 100;*/


--how many are missing at least one demographic
with get_data as (
    SELECT a.*,
           age_range,
           gender,
           acorn_category || '-' || acorn_category_description as acorn_category,
           nation,
           segvalue
    FROM vb_chrys_feat_test a
             LEFT JOIN prez.profile_extension b on a.audience_id = b.bbc_hid3
             LEFT JOIN central_insights.segserver_SG10122_geo_proxy_1 c on a.audience_id = c.hashedid
)
SELECT *
FROM get_data
WHERE age_range ISNULL
   OR gender ISNULL
   OR acorn_category ISNULL;


SELECT cluster,
FROM vb_cluster_demo
ORDER BY cluster, nation, age_range, gender, acorn_category
LIMIT 10;