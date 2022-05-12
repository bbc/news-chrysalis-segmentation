import pandas as pd
from db_query import Databases

from sklearn.decomposition import PCA
from sklearn.preprocessing import MinMaxScaler
from sklearn.pipeline import FeatureUnion, Pipeline
from sklearn.cluster import KMeans

from joblib import load


TABLE_NAME = "central_insights_sandbox.ed_current_data_to_segment"
MODEL_FP = "models/trained_model.joblib"
SCHEMA_NAME = "central_insights_sandbox"
OUT_TABLE = "ed_uk_user_taste_segments"

# SQL query for pulling out features
SQL_QUERY = f"""
SELECT audience_id, page_section, topic_perc
FROM {TABLE_NAME}
UNION
SELECT DISTINCT 'dummy'::varchar as audience_id, page_section, 0::double precision as topic_perc FROM {TABLE_NAME} ORDER BY 2;
""".strip()


if __name__ == '__main__':
    db = Databases()
    feature_table = db.read_from_db(SQL_QUERY)
    feature_table = feature_table.set_index(['audience_id', 'page_section'])
    feature_table = feature_table.unstack('page_section', fill_value=0)
    feature_table = feature_table.loc[feature_table.index != 'dummy']

    print(f'Read in features: {feature_table.shape}')

    # Load the model in
    pipe = load(MODEL_FP)

    print('Loaded model')

    # Use the pipeline to predict labels for each user in the data loaded in
    labels = pipe.predict(feature_table.values)
    labels = pd.Series(labels, index=feature_table.index)

    # Convert labels to dataframe for writing to SQL table
    labels = labels.reset_index()
    labels.columns = ['audience_id', 'segment']

    # Write the labels to a redshift table
    # This might need pointing to a segserver at some point but I have no idea how to do that
    db.write_df_to_db(labels, SCHEMA_NAME, OUT_TABLE)

    print('Saved user labels')
