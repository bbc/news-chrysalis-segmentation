import pandas as pd
from db_query import Databases

from sklearn.decomposition import PCA
from sklearn.preprocessing import MinMaxScaler, StandardScaler
from sklearn.pipeline import FeatureUnion, Pipeline
from sklearn.cluster import KMeans

from joblib import dump

TABLE_NAME = "central_insights_sandbox.ed_current_data_to_segment"
MODEL_FP = "models/trained_model.joblib"

# SQL query for pulling out features
SQL_QUERY = f"""
SELECT audience_id, page_section, topic_perc
FROM {TABLE_NAME}
WHERE audience_id IN
      (SELECT DISTINCT audience_id FROM {TABLE_NAME} ORDER BY RANDOM() LIMIT 1000000)
UNION
SELECT DISTINCT 'dummy'::varchar as audience_id, page_section, 0::double precision as topic_perc FROM {TABLE_NAME} ORDER BY 2;
""".strip()


if __name__ == '__main__':
    # Read in the feature table for training the model
    db = Databases()
    feature_table = db.read_from_db(SQL_QUERY)
    feature_table = feature_table.set_index(['audience_id', 'page_section'])
    feature_table = feature_table.unstack('page_section', fill_value=0)
    feature_table = feature_table.loc[feature_table.index != 'dummy']

    print(f'Read in features: {feature_table.shape}')

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

    print('Dumped model')
    