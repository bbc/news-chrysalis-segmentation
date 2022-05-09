import pandas as pd
from db_query import Databases

from sklearn.decomposition import PCA
from sklearn.preprocessing import MinMaxScaler
from sklearn.pipeline import FeatureUnion, Pipeline
from sklearn.cluster import KMeans

from joblib import dump


SQL_COLS = [
    "audience_id", 
    "totalnews",
    "world_prop",
    "england_prop",
    "uk_prop",
    "politics_prop",
    "business_prop",
    "scotland_prop",
    "entertainment_prop",
    "wales_prop",
    "technology_prop",
    "newsbeat_prop",
    "health_prop",
    "science_prop",
    "n_ireland_prop",
    "education_prop",
    "sport_prop",
    "disability_prop"
]

# SQL query for pulling out features
SQL_QUERY = """
SELECT {} FROM central_insights_sandbox.ed_uk_news_seg_features 
ORDER BY RANDOM() 
LIMIT 100000;
""".format(', '.join(SQL_COLS)).strip()


if __name__ == '__main__':
    # Read in the feature table for training the model
    db = Databases()
    feature_table = db.read_from_db(SQL_QUERY)
    feature_table = feature_table.set_index('audience_id')

    print(f'Read in features: {feature_table.shape}')

    # SKLearn pipeline which scales, reduces, and clusters the features it is given
    pipe = Pipeline([
                        ('Scaler', MinMaxScaler()),
                        ('PCA', PCA(n_components=5)),
                        ('Cluster', KMeans(n_clusters=5, random_state=0))
                    ])
    
    # Fit the pipeline on the feature table
    pipe.fit(feature_table.values)

    print('Fitted model')

    # Dump the fitted pipeline to a file
    dump(pipe, 'trained_model.joblib')

    print('Dumped model')
    