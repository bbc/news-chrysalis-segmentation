BEGIN;
set search_path TO 'central_insights_sandbox';

DROP TABLE IF EXISTS ed_uk_user_taste_segments;

CREATE TABLE ed_uk_user_taste_segments
    (
        audience_id VARCHAR(100),
        segment INT
    );

COPY ed_uk_user_taste_segments
FROM '<params.s3_file_location_opens>'
CREDENTIALS 'aws_access_key_id=<params.AWS_ACCESS_KEY_ID>;aws_secret_access_key=<params.AWS_SECRET_ACCESS_KEY>;token=<params.TOKEN>'
ENCODING UTF16
IGNOREHEADER AS 1
DELIMITER AS '|'
EMPTYASNULL
FILLRECORD
TRUNCATECOLUMNS
REMOVEQUOTES
MAXERROR 1500
;

END