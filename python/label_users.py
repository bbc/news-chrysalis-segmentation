import pandas as pd
from db_query import Databases

from sklearn.decomposition import PCA
from sklearn.preprocessing import MinMaxScaler
from sklearn.pipeline import FeatureUnion, Pipeline
from sklearn.cluster import KMeans

from joblib import load
import requests
import boto3
import json
import numpy as np
import logging
import os

from utilities import download_from_s3, upload_to_s3


# TABLE_NAME = "central_insights_sandbox.ed_current_data_to_segment"
# # TEST_SAMPLE_TABLE_NAME = "central_insights_sandbox.ed_current_segmentation_test_sample"
# MODEL_FP = "models/trained_model.joblib"
# FEATURE_NAMES_FP = "models/features.json"
# # SCHEMA_NAME = "central_insights_sandbox"
# # OUT_TABLE = "ed_uk_user_taste_segments"
# TEMP_DATA_DUMP = "temp_data_dump.parquet.gzip"
# BATCH_SIZE = 1000000

TABLE_NAME = os.environ.get('table_name')
MODEL_FP = os.environ.get('model_fp')
FEATURE_NAMES_FP = os.environ.get('feature_names_fp')
TEMP_DATA_DUMP = os.environ.get('temp_data_dump')
BATCH_SIZE = int(os.environ.get('batch_size'))
BUCKET_NAME = os.environ.get('bucket_name')
BUCKET_FOLDER = os.environ.get('bucket_folder')

# # SQL query for pulling out features
# SQL_QUERY = f"""
# SELECT audience_id, page_section, topic_perc
# FROM {TABLE_NAME}
# UNION
# SELECT DISTINCT 'dummy'::varchar as audience_id, page_section, 0::double precision as topic_perc FROM {TABLE_NAME} ORDER BY 2;
# """.strip()

def generate_sql_query(cur_offset):
    # SQL query for pulling out features
    query = f"""
    SELECT audience_id, page_section, topic_perc
    FROM {TABLE_NAME}
    WHERE audience_id IN
        (SELECT DISTINCT audience_id FROM {TABLE_NAME} ORDER BY audience_id LIMIT {BATCH_SIZE} OFFSET {cur_offset})
    UNION
    SELECT DISTINCT 'dummy'::varchar as audience_id, page_section, 0::double precision as topic_perc FROM {TABLE_NAME} ORDER BY 2;
    """.strip()

    return query


# role_name = requests.get('http://169.254.169.254/latest/meta-data/iam/security-credentials/').text
# s3credentials = requests.get('http://169.254.169.254/latest/meta-data/iam/security-credentials/' + role_name).json()


def align_feature_table_to_model(feature_table, feature_names):
    """
    Function which gives a feature table the given columns.
    Fills any missing columns with zeros and excludes any additional columns
    """
    # Get the features that are present
    present_features = [f for f in feature_names if f in feature_table.columns]
    # Get the features that are missing
    missing_features = [f for f in feature_names if f not in feature_table.columns]

    # Make a new feature table with the incorrect features removed
    new_feature_table = feature_table.loc[:, present_features]

    # Add the missing columns, filling with zeros
    if len(missing_features) > 0:
        new_feature_table.loc[:, missing_features] = np.zeros((feature_table.shape[0], len(missing_features)))

    # Match the order of the original feature table so the model works properly
    new_feature_table = new_feature_table.loc[:, feature_names]

    return new_feature_table


if __name__ == '__main__':
    logging.debug("STARTING...")
    db = Databases()
    engine = db.create_connector()

    num_users = engine.execute(f"SELECT COUNT(*) FROM (SELECT DISTINCT audience_id FROM {TABLE_NAME});")
    num_users = num_users.fetchall()[0][0]
    num_batches = int(num_users / BATCH_SIZE) + 1
    logging.debug(f"Using {num_batches} batches")

    # # Drop the current labels table
    # db.write_to_db(f"DROP TABLE IF EXISTS {SCHEMA_NAME}.{OUT_TABLE};")
    # # Create a new labels table
    # create_table_sql = f"""
    # CREATE TABLE {SCHEMA_NAME}.{OUT_TABLE}
    # (
    #     audience_id VARCHAR(100),
    #     segment INT
    # );
    # """
    # db.write_to_db(create_table_sql)

    # Download the feature names
    download_from_s3(FEATURE_NAMES_FP, 'map-input-output', 'chrysalis-taste-segmentation/features.json')
    logging.debug('Downloaded feature neames from s3')

    # Load the feature names
    with open(FEATURE_NAMES_FP, 'r', encoding='utf-8') as feat_file:
        feature_names = json.load(feat_file)
    logging.debug('Loaded feature names')

    # Download the model
    download_from_s3(MODEL_FP, 'map-input-output', 'chrysalis-taste-segmentation/trained_model.joblib')
    logging.debug('Downloaded Model from S3')
    # Load the model in
    pipe = load(MODEL_FP)
    logging.debug('Loaded model')

    batch_labels = []


    for cur_batch_num in range(num_batches):
        logging.debug("-------------------------------------")
        logging.debug(f"BATCH NUMBER {cur_batch_num}")
        logging.debug("-------------------------------------")

        cur_offset = BATCH_SIZE * cur_batch_num

        cur_query = generate_sql_query(cur_offset)
        feature_table = db.read_from_db(cur_query)
        logging.debug('Read database')
        feature_table = feature_table.set_index(['audience_id', 'page_section'])    # Set the index of the data read from Redshift
        logging.debug('Reset index')
        feature_table = feature_table.unstack('page_section', fill_value=0)         # Move the unique values of section up to become columns
        logging.debug('Unstacked rows to columns')
        feature_table = feature_table.loc[feature_table.index != 'dummy']           # Remove the dummy rows added in to ensure all features are gathered
        logging.debug('Removed dummy entries')
        feature_table.columns = feature_table.columns.droplevel(0)                  # Drop the weird extra column level pandas adds in
        logging.debug('Simplified column names')

        logging.debug(f'Read in features: {feature_table.shape}')

        # Align this table so it has the same features in the same order as the original
        feature_table = align_feature_table_to_model(feature_table, feature_names)
        logging.debug('Aligned feature table with feature names')

        # Use the pipeline to predict labels for each user in the data loaded in
        labels = pipe.predict(feature_table.values)
        logging.debug('Performed labelling')
        labels = pd.Series(labels, index=feature_table.index)
        logging.debug('Made labels into a series')

        # Convert labels to dataframe for writing to SQL table
        labels = labels.reset_index()
        labels.columns = ['audience_id', 'segment']
        logging.debug('Formatted data for dumping to redshift')

        batch_labels.append(labels)

        # # Write the labels to a redshift table
        # # This might need pointing to a segserver at some point but I have no idea how to do that
        # print("Writing to redshift")
        # db.write_df_to_db(labels, SCHEMA_NAME, OUT_TABLE)
        # print('Saved user labels to Redshift')

    # Concatenate all the labels
    logging.debug('Merging batches')
    labels = pd.concat(batch_labels, axis=0).reset_index(drop=True)
    # Write the dataframe to a file
    logging.debug("Dumping to local file")
    labels.to_parquet(TEMP_DATA_DUMP, compression='gzip')
    # Upload said file to s3
    logging.debug('Upload to S3')
    upload_to_s3(TEMP_DATA_DUMP, BUCKET_NAME, f'{BUCKET_FOLDER}/{TEMP_DATA_DUMP}')

    # # Grant permissions so I can check the data from my account
    # db.write_to_db(f'GRANT ALL ON {SCHEMA_NAME}.{OUT_TABLE} TO edward_dearden WITH GRANT OPTION;')
    # print("Permissions Granted")
    logging.debug("FINISHED")