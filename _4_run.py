from _1_stockdata_extract   import StockDataExtraction
from _2_database_insert     import SQL_insert
from _3_database            import Database 

engine = Database().sql_engine()
tblname = 'stockdata'        
start = '2025-01-01'
end = '2025-02-18'
data = StockDataExtraction(start = start,end = end, freq = '15m', )
df = data.RunExtract()
si = SQL_insert(tblname, df, engine)
si.database_insert()
