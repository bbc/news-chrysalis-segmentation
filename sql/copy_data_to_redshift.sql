BEGIN;
DROP TABLE IF EXISTS <params.table_name>;

CREATE TABLE <params.table_name>
    (
        audience_id VARCHAR(100),
        segment INT
    );

COPY <params.table_name>
FROM '<params.s3_file_location>'
CREDENTIALS 'aws_access_key_id=<params.AWS_ACCESS_KEY_ID>;aws_secret_access_key=<params.AWS_SECRET_ACCESS_KEY>;token=<params.TOKEN>'
PARQUET
GZIP
-- IGNOREHEADER AS 1
-- MAXERROR 1500
;

GRANT ALL ON <params.table_name> TO edward_dearden WITH GRANT OPTION;
GRANT ALL ON <params.table_name> TO vicky_banks WITH GRANT OPTION;

END