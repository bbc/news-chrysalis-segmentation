import pandas as pd
from db_query import Databases

from sklearn.decomposition import PCA
from sklearn.preprocessing import MinMaxScaler, StandardScaler
from sklearn.pipeline import FeatureUnion, Pipeline
from sklearn.cluster import KMeans

from joblib import dump
import requests
import boto3
import json
import logging

from utilities import upload_to_s3


TABLE_NAME = "central_insights_sandbox.ed_current_data_to_segment"
MODEL_FP = "models/trained_model.joblib"
FEATURE_NAMES_FP = "models/features.json"
DUMP_FILE = "training_user_segments.parquet.gzip"

# SQL query for pulling out features
SQL_QUERY = f"""
SELECT audience_id, page_section, topic_perc
FROM {TABLE_NAME}
WHERE audience_id IN
      (SELECT DISTINCT audience_id FROM {TABLE_NAME} ORDER BY RANDOM() LIMIT 1000000)
UNION
SELECT DISTINCT 'dummy'::varchar as audience_id, page_section, 0::double precision as topic_perc FROM {TABLE_NAME} ORDER BY 2;
""".strip()


role_name = requests.get('http://169.254.169.254/latest/meta-data/iam/security-credentials/').text
s3credentials = requests.get('http://169.254.169.254/latest/meta-data/iam/security-credentials/' + role_name).json()


if __name__ == '__main__':
    # Read in the feature table for training the model
    db = Databases()
    feature_table = db.read_from_db(SQL_QUERY)
    feature_table = feature_table.set_index(['audience_id', 'page_section'])    # Set the index of the data read from Redshift
    feature_table = feature_table.unstack('page_section', fill_value=0)         # Move the unique values of section up to become columns
    feature_table = feature_table.loc[feature_table.index != 'dummy']           # Remove the dummy rows added in to ensure all features are gathered
    feature_table.columns = feature_table.columns.droplevel(0)                  # Drop the weird extra column level pandas adds in


    logging.debug(f'Read in features: {feature_table.shape}')

    # Get the feature names and dump them to a file
    feature_names = list(feature_table.columns)
    with open(FEATURE_NAMES_FP, 'w', encoding='utf-8') as feat_file:
        json.dump(feature_names, feat_file)
    
    # Dump the features into s3
    upload_to_s3(FEATURE_NAMES_FP, 'map-input-output', 'chrysalis-taste-segmentation/features.json')

    logging.debug("Dumped feature names to s3")
    
    # SKLearn pipeline which scales, reduces, and clusters the features it is given
    pipe = Pipeline([
                        ('Scaler', StandardScaler()),
                        ('PCA', PCA(n_components=5)),
                        ('Cluster', KMeans(n_clusters=5, random_state=0))
                    ])
    
    # Fit the pipeline on the feature table
    pipe.fit(feature_table.values)

    logging.debug('Fitted model')

    # Dump the fitted pipeline to a file
    dump(pipe, MODEL_FP)
    # Upload pipeline to s3
    upload_to_s3(MODEL_FP, 'map-input-output', 'chrysalis-taste-segmentation/trained_model.joblib')

    logging.debug('Dumped model')
    
    # Save the segments of the training data
    logging.debug("Dumping the segments for the training data to s3")
    # Use the pipeline to predict labels for each user in the data loaded in
    labels = pipe.predict(feature_table.values)
    logging.debug('Performed labelling')
    labels = pd.Series(labels, index=feature_table.index)
    logging.debug('Made labels into a series')

    # Convert labels to dataframe for writing to SQL table
    labels = labels.reset_index()
    labels.columns = ['audience_id', 'segment']
    logging.debug('Formatted data for dumping to redshift')

    # Write the dataframe to a file
    logging.debug("Dumping to local file")
    labels.to_parquet(DUMP_FILE, compression='gzip')

    # Upload said file to s3
    logging.debug('Upload to S3')
    upload_to_s3(DUMP_FILE, 'map-input-output', f'chrysalis-taste-segmentation/{DUMP_FILE}')

    logging.debug("Finished")
