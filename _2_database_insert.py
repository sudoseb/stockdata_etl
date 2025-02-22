import pandas as pd
from _1_stockdata_extract import StockDataExtraction
from _3_database import Database 
import datetime
import sqlalchemy
from sqlalchemy import create_engine,text

class SQL_insert:
    '''
    Able to create tables and inserting time series data based on format given from _1_stockdata_extract.py class into given database
    '''
    def __init__(self, tblname, df, engine) -> None:
        self.tblname = tblname
        self.df = df
        self.engine = engine
        
    def database_insert(self) -> None:
        print('Start!')
        
        # Transform and process the DataFrame based on data from stockdataextraction class
        df = self.Transform()
        
        #get current existing data
        existing_data = pd.read_sql(f'SELECT ticker, datetime FROM {self.tblname}', self.engine)

        #Horrible readability syntax... but it just applying rowwise tuple check between both dfs. massive room for improvement.  
        new_data = df[~df[['ticker', 'datetime']].apply(tuple, axis = 1).isin(existing_data[['ticker', 'datetime']].apply(tuple, axis = 1))]

        # If new data exists, append it to the SQL table
        if not new_data.empty:
            new_data = new_data.dropna()  # Remove any rows with NaN values
            new_data.to_sql(self.tblname, self.engine, schema='dbo', if_exists='append', index=False)
            print(f'{len(new_data)} new rows added to {self.tblname}.')
        else:
            print('No new data to append.')
        
    def getDatatypes(self) -> dict:
        #CHATGTP --- Probably not robust but broad, regular "strings" or stocknames will automatically be of dtype "Object". 
        pandas_to_sql_types = {
        'object': 'VARCHAR(50)',
        'string': 'VARCHAR(255)',
        'float64': 'DOUBLE PRECISION',
        'float32': 'FLOAT',
        'int64': 'BIGINT',
        'int32': 'INTEGER',
        'int16': 'SMALLINT',
        'int8': 'SMALLINT',  # Assuming TINYINT is not supported --
        'uint64': 'BIGINT',
        'uint32': 'INTEGER',
        'uint16': 'SMALLINT',
        'uint8': 'SMALLINT',  # Assuming TINYINT is not supported -- 
        'datetime64[ns]': 'TIMESTAMP',
        'datetime64[ns, UTC]': 'Datetime2',
        'datetime64[ns, Europe/Stockholm]': 'Datetime2',  # Replace <timezone> with the specific timezone
        'timedelta64[ns]': 'INTERVAL',
        'bool': 'BOOLEAN',
        'category': 'VARCHAR',  # Usually used for categorical data
        'Sparse[int]': 'INTEGER',  # Depending on the sparse data type
        'Sparse[float]': 'DOUBLE PRECISION',  # Depending on the sparse data type
        }
        
        collist = self.df.columns
        types = self.df.dtypes
        d = {}
        for col in collist:
            pandas_type = str(types[col])
            sql_type = pandas_to_sql_types.get(pandas_type, 'VARCHAR(255)') 
            d[col] = sql_type
        return d
    def CreateTables(self) -> None:
        #self.df['created_at'] = pd.to_datetime('now')
        collist = self.df.columns
        dtypes = self.getDatatypes()
        query = f"CREATE TABLE {self.tblname} ({', '.join([f'{col} {dtypes[col]}' for col in collist])});"
        with self.engine.connect() as conn:
            conn.execute(text(query))
            conn.commit()
    def Transform(self) -> object:
        tl = StockDataExtraction.read_file()
        tickerlist = [i.replace('.ST','').replace('-','_') for i in tl]

        # UNPIVOT AND DEFINE TYPE
        self.df = self.df.reset_index()
        if 'Date' in self.df.columns:
            self.df = self.df.rename(columns = {'Date':'datetime'})
            self.df['datetime'] = pd.to_datetime(self.df['datetime']) + pd.to_timedelta('17:15:00')
            self.df['datetime'] = self.df['datetime'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        else: 
            self.df = self.df.rename(columns = {'Datetime':'datetime'})

        df_unpivoted = pd.melt(self.df, id_vars=['datetime'], var_name='stockname', value_name='stockprice')
        df_unpivoted['ticker'] = df_unpivoted['stockname'].str.replace('.ST', '').str.replace('-','_')
        df_unpivoted = df_unpivoted.ffill()
        df_unpivoted = df_unpivoted.drop('stockname', axis= 1)
        df_unpivoted['tickertype'] = df_unpivoted['ticker'].apply(lambda x: 'Stock' if x in tickerlist else 'Cryptocurrency')

        return df_unpivoted



