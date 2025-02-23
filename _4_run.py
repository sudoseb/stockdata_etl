from _1_stockdata_extract   import StockDataExtraction
from _2_database_insert     import SQL_insert
from _3_database            import Database 

engine = Database().sql_engine()
tblname = 'PROD_stockdata'        
start = '2025-02-22'
end = '2025-02-23'
data = StockDataExtraction(start = start,end = end, freq = '15m',CryptoCount=200)
df = data.RunExtract()
si = SQL_insert(tblname, df, engine)
si.database_insert()
