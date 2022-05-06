import datetime
import os
import pandas as pd
import re


from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError


class Databases:

    query = ""
    result = pd.DataFrame()

    def __init__(self):
        self.redshift_host = 'localhost'
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

    def write_df_to_db(self, df, schema, table):
        """
        Writes DataFrame to database.
        :param df: DataFrame
        :param schema: String
        :param table: String
        :return: None
        """
        insert_string_base = f"INSERT INTO {schema}.{table} VALUES"
        insert_string = insert_string_base
        row_limit_per_insert = 5000
        db = Databases.create_connector(self)
        db.connect()
        df_len = len(df)
        i = 0
        j = 0
        for index, row in df.iterrows():
            value_string = ""
            for item in row:
                value_string += f"'{item}',"
                i += 1
            value_string = value_string[:-1]
            insert_string += f"({value_string}), "
            if j % row_limit_per_insert == 0:
                insert_string = insert_string[:-2]
                try:
                    print(
                        f"[{datetime.datetime.now()}][PAGESINDEX] "
                        f"Processing index insert: {j} / {df_len}"
                    )
                    print("============================")
                    db.execute(insert_string)
                except:
                    print(
                        f"[{datetime.datetime.now()}][PAGESINDEX] "
                        f"Failed index insert: {j} / {df_len}"
                    )
                    continue
                insert_string = insert_string_base
            if j > df_len:
                break
            j += 1
        insert_string = insert_string[:-2]
        try:
            print("============================")
            db.execute(insert_string)
        except ProgrammingError:
            print("")
        print(f"[{datetime.datetime.now()}][PAGESINDEX] COMPLETE")
        db.dispose()