from sqlalchemy import column
from db_query import Databases

drop_query = 'DROP TABLE IF EXISTS central_insights_sandbox.ed_page_topics_pivoted'

make_pivot_query = '''
CREATE TABLE central_insights_sandbox.ed_page_topics_pivoted as
SELECT * FROM central_insights_sandbox.ed_page_topics
PIVOT
(
    SUM(topic_count) AS sum FOR page_section IN {0}
);
'''.strip()

get_columns_query = 'SELECT DISTINCT page_section FROM central_insights_sandbox.ed_page_topics;'

if __name__ == '__main__':
    database = Databases()
    db = Databases.create_connector(database)
    db.connect()
    result_proxy = db.execute(get_columns_query)
    result = result_proxy.fetchall()
    column_names = [f"'{x[0]}'" for x in result]
    column_names = f"({', '.join(column_names)})"
    db.execute(drop_query)
    db.execute(make_pivot_query.format(column_names))
    db.dispose()