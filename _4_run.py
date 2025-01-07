from _1_stockdata_extract   import StockDataExtraction
from _2_database_insert     import SQL_insert
from _3_database            import Database 

engine = Database().sql_engine()
tblname = 'stockdata'        
start = '2018-11-05'
end = '2024-01-01'
data = StockDataExtraction(start = start,end = end, freq = '1d', )
df = data.RunExtract()
si = SQL_insert(tblname, df, engine)
si.database_insert()
