import chunk
import datetime
import os
import pandas as pd
import re
import logging


from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.exc import ProgrammingError

from utilities import upload_to_s3


class Databases:

    query = ""
    result = pd.DataFrame()

    def __init__(self):
        self.redshift_host = 'live-idl-prod-redshift-component-redshiftcluster-1q6vyltqf8lth.ctm1v7db0ubd.eu-west-1.redshift.amazonaws.com'
        self.redshift_user = os.environ.get('REDSHIFT_USR')
        self.redshift_pw = os.environ.get('REDSHIFT_PSD')
        self.redshift_port = os.environ.get('REDSHIFT_PRT')
        self.redshift_db = 'redshiftdb'

    def create_connector(self):
        """
        Create the connection engine using your own credentials
        :return: engine
        """
        connect_to_db_query = (
            "postgresql+psycopg2://"
            + self.redshift_user
            + ":"
            + self.redshift_pw
            + "@"
            + self.redshift_host
            + ":"
            + self.redshift_port
            + "/"
            + self.redshift_db
        )
        engine = create_engine(connect_to_db_query)
        return engine

    def read_from_db(self, query):
        """
        Connects the engine to the database using the engine string and your
        credentials. this will also execute your query and return the result in a
        pandas dataframe format
        :param query: String
        :return: DataFrame
        """
        db = Databases.create_connector(self)
        db.connect()
        result_proxy = db.execute(query)
        self.result = pd.DataFrame(
            result_proxy.fetchall(),
            columns=result_proxy.keys(),
        )
        db.dispose()
        return self.result

    def write_to_db(self, query):
        """
        Connects the engine to the database using the engine string and your
        credentials. this will also execute your query which writes the selected
        data to the database.
        :param query: String
        :return: Boolean
        """
        db = Databases.create_connector(self)
        db.connect()
        db.execute(text(query))
        db.dispose()
        return True


    def string_base(self, x):
        """
        Formats strings using regex ready for inserts.
        :param x: String
        :return: String
        """
        return re.sub(r"[^A-Za-z0-9]+", "", x).lower()


    def chunk_df(self, df, chunk_size):
        cur = []
        for i, (index, row) in enumerate(df.iterrows()):
            # If we're on a multiple of chunk_size, yield the current list
            if i % chunk_size == 0 and len(cur) > 0:
                yield cur
                cur = []
            # Add the current row
            cur.append(list(row))
        # Yield whatever's left
        if len(cur) > 0:
            yield cur


    def test_insert_string(self, df, schema, table):
        """
        Tests the insert string creation
        """
        # Creat the base query string
        insert_string_base = f"INSERT INTO {schema}.{table} VALUES "
        row_limit_per_insert = 5

        for cur_chunk in self.chunk_df(df, row_limit_per_insert):
            insert_string = insert_string_base + ", ".join([f'({", ".join([str(x) for x in row])})' for row in cur_chunk]) + ';'
            print(insert_string)


    def write_df_to_db(self, df, schema, table):
        """
        Writes DataFrame to database.
        :param df: DataFrame
        :param schema: String
        :param table: String
        :return: None
        """
        # Creat the base query string
        insert_string_base = f"INSERT INTO {schema}.{table} VALUES "
        row_limit_per_insert = 5000
        df_len = df.shape[0]
        db = Databases.create_connector(self)
        db.connect()

        for cur_chunk in self.chunk_df(df, row_limit_per_insert):
            insert_string = insert_string_base + ", ".join([f'({", ".join([str(x) for x in row])})' for row in cur_chunk]) + ';'
            db.execute(insert_string)
            # try:
            #     db.execute(insert_string)
            # except Exception as e:
            #     logging.error(str(e))
            #     continue

        logging.debug(f"[{datetime.datetime.now()}][PAGESINDEX] COMPLETE")
        db.dispose()