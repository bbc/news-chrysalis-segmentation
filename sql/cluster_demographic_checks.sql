set search_path TO 'central_insights_sandbox';

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
           nation
    FROM vb_chrys_feat_test a
             LEFT JOIN prez.profile_extension b on a.audience_id = b.bbc_hid3
)
SELECT cluster, age_range, gender, acorn_category, nation, count(audience_id) as users
FROM get_data
GROUP BY 1, 2, 3, 4, 5
;

SELECT cluster, age_range, count(audience_id) as users
FROM vb_cluster_demo
GROUP BY 1, 2
ORDER BY 1, 2;

SELECT * FROM vb_cluster_demo ORDER BY cluster, nation, age_range, gender, acorn_category;
SELECT distinct age_range FROM prez.profile_extension

--- Who are the people in the boring cluster?
SELECT *
FROM vb_chrys_feat_test
WHERE cluster = 0
LIMIT 10;

SELECT product,date_of_segmentation, bbc_hid3, segvalue
FROM central_insights.sg10026_info_individual_alltime
WHERE product = 'news'
AND date_of_segmentation BETWEEN (SELECT min_date FROM central_insights_sandbox.vb_dates) AND (SELECT max_date FROM central_insights_sandbox.vb_dates)
AND bbc_hid3 IN (SELECT DISTINCT audience_id FROM vb_chrys_feat_test)
ORDER BY bbc_hid3, date_of_segmentation
LIMIT 100;