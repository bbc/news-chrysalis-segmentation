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


TABLE_NAME = "central_insights_sandbox.ed_current_data_to_segment"
# TEST_SAMPLE_TABLE_NAME = "central_insights_sandbox.ed_current_segmentation_test_sample"
MODEL_FP = "models/trained_model.joblib"
FEATURE_NAMES_FP = "models/features.json"
SCHEMA_NAME = "central_insights_sandbox"
OUT_TABLE = "ed_uk_user_taste_segments"

# # SQL query for pulling out features
# SQL_QUERY = f"""
# SELECT audience_id, page_section, topic_perc
# FROM {TABLE_NAME}
# UNION
# SELECT DISTINCT 'dummy'::varchar as audience_id, page_section, 0::double precision as topic_perc FROM {TABLE_NAME} ORDER BY 2;
# """.strip()

# SQL query for pulling out features
SQL_QUERY = f"""
SELECT audience_id, page_section, topic_perc
FROM {TABLE_NAME}
WHERE audience_id IN
      (SELECT DISTINCT audience_id FROM {TABLE_NAME} ORDER BY RANDOM() LIMIT 500000)
UNION
SELECT DISTINCT 'dummy'::varchar as audience_id, page_section, 0::double precision as topic_perc FROM {TABLE_NAME} ORDER BY 2;
""".strip()

role_name = requests.get('http://169.254.169.254/latest/meta-data/iam/security-credentials/').text
s3credentials = requests.get('http://169.254.169.254/latest/meta-data/iam/security-credentials/' + role_name).json()

def download_from_s3(local_file_path, bucket_name, bucket_filepath):
   s3 = boto3.client('s3')
   with open(local_file_path, "wb") as f:
       s3.download_fileobj(bucket_name, bucket_filepath, f)


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
    print("Starting...")
    db = Databases()
    feature_table = db.read_from_db(SQL_QUERY)
    print('Read database')
    feature_table = feature_table.set_index(['audience_id', 'page_section'])    # Set the index of the data read from Redshift
    print('Reset index')
    feature_table = feature_table.unstack('page_section', fill_value=0)         # Move the unique values of section up to become columns
    print('Unstacked rows to columns')
    feature_table = feature_table.loc[feature_table.index != 'dummy']           # Remove the dummy rows added in to ensure all features are gathered
    print('Removed dummy entries')
    feature_table.columns = feature_table.columns.droplevel(0)                  # Drop the weird extra column level pandas adds in
    print('Simplified column names')

    print(f'Read in features: {feature_table.shape}')

    # Download the feature names
    download_from_s3(FEATURE_NAMES_FP, 'map-input-output', 'chrysalis-taste-segmentation/features.json')
    print('Downloadwed feature neames from s3')

    # Load the feature names
    with open(FEATURE_NAMES_FP, 'r', encoding='utf-8') as feat_file:
        feature_names = json.load(feat_file)
    print('Loaded feature names')

    # Align this table so it has the same features in the same order as the original
    feature_table = align_feature_table_to_model(feature_table, feature_names)
    print('Aligned feature table with feature names')

    # Download the model
    download_from_s3(MODEL_FP, 'map-input-output', 'chrysalis-taste-segmentation/trained_model.joblib')
    print('Downloaded Model from S3')
    # Load the model in
    pipe = load(MODEL_FP)
    print('Loaded model')

    # Use the pipeline to predict labels for each user in the data loaded in
    labels = pipe.predict(feature_table.values)
    print('Performed labelling')
    labels = pd.Series(labels, index=feature_table.index)
    print('Made labels into a series')

    # Convert labels to dataframe for writing to SQL table
    labels = labels.reset_index()
    labels.columns = ['audience_id', 'segment']
    print('Formatted data for dumping to redshift')

    # Write the labels to a redshift table
    # This might need pointing to a segserver at some point but I have no idea how to do that
    db.write_df_to_db(labels, SCHEMA_NAME, OUT_TABLE)
    db.write_to_db(f'GRANT ALL ON {SCHEMA_NAME}.{OUT_TABLE} TO edward_dearden WITH GRANT OPTION;')
    print('Saved user labels to Redshift')
