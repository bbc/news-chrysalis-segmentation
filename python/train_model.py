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

from utilities import upload_to_s3


TABLE_NAME = "central_insights_sandbox.ed_current_data_to_segment"
MODEL_FP = "models/trained_model.joblib"
FEATURE_NAMES_FP = "models/features.json"

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


    print(f'Read in features: {feature_table.shape}')

    # Get the feature names and dump them to a file
    feature_names = list(feature_table.columns)
    with open(FEATURE_NAMES_FP, 'w', encoding='utf-8') as feat_file:
        json.dump(feature_names, feat_file)
    
    # Dump the features into s3
    upload_to_s3(FEATURE_NAMES_FP, 'map-input-output', 'chrysalis-taste-segmentation/features.json')

    print("Dumped feature names to s3")
    
    # SKLearn pipeline which scales, reduces, and clusters the features it is given
    pipe = Pipeline([
                        ('Scaler', StandardScaler()),
                        ('PCA', PCA(n_components=5)),
                        ('Cluster', KMeans(n_clusters=5, random_state=0))
                    ])
    
    # Fit the pipeline on the feature table
    pipe.fit(feature_table.values)

    print('Fitted model')

    # Dump the fitted pipeline to a file
    dump(pipe, MODEL_FP)
    # Upload pipeline to s3
    upload_to_s3(MODEL_FP, 'map-input-output', 'chrysalis-taste-segmentation/trained_model.joblib')

    print('Dumped model')
    