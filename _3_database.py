from sqlalchemy import create_engine, text
import pandas as pd
class Database:
    '''
    Takes database params and can return both engine for sqlalchemy, return a df based on query from db or execute a sql query.
    '''
    def __init__(self, driver='{ODBC Driver 17 for SQL Server}', server='local', database='master',table='PROD_stockdata', sql_query='*') -> None:
        self.driver = driver
        self.server = server
        self.database = database
        self.table = table
        self.sql_query = sql_query
    def sql_engine(self) -> object:
        connstring = (
        f'driver={self.driver};'
        f'server=({self.server});'
        f'database={self.database};'
        f'trusted_connection=yes;'
        )
        return create_engine(f"mssql+pyodbc:///?odbc_connect={connstring}")

    def df(self) -> object:
        if self.sql_query == '*': 
            return pd.read_sql(f"SELECT * FROM {self.table}", con=self.sql_engine())
        else: 
            return pd.read_sql(self.sql_query, con=self.sql_engine())
    def db_query(self) -> None:
        with self.engine.connect() as conn:
            conn.execute(text(self.sql_query))

