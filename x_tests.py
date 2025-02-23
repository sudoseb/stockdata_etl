from _1_stockdata_extract   import StockDataExtraction
from _2_database_insert     import SQL_insert
from _3_database            import Database 
df = Database(sql_query='select top 1 * from PROD_stockdata;').df()
import pandas as pd
import matplotlib.pyplot as plt



print(df)