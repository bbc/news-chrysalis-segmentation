import pandas as pd
from db_query import Databases

from sklearn.decomposition import PCA
from sklearn.preprocessing import MinMaxScaler
from sklearn.pipeline import FeatureUnion, Pipeline
from sklearn.cluster import KMeans

from joblib import load


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

SQL_QUERY = """
SELECT {} FROM central_insights_sandbox.ed_uk_news_seg_features 
ORDER BY RANDOM() 
LIMIT 10;
""".format(', '.join(SQL_COLS)).strip()


if __name__ == '__main__':
    db = Databases()
    feature_table = db.read_from_db(SQL_QUERY)
    feature_table = feature_table.set_index('audience_id')

    print(f'Read in features: {feature_table.shape}')

    pipe = load('trained_model.joblib')

    print('Loaded model')

    labels = pipe.predict(feature_table.values)
    labels = pd.Series(labels, index=feature_table.index)
    
    labels.to_csv('user_labels.csv')

    print('Saved user labels')
