
from yahooquery import Screener
import pandas as pd
import yfinance as yf
import re

class StockDataExtraction:
    '''
    Extracting data from static file 
    '''
    def __init__(self, start, end, freq='15m', CryptoCount=200,metric='Adj Close', tickerlist= []) -> None:
        self.start = start
        self.end = end
        self.freq = freq
        self.count = CryptoCount
        self.metric = metric
        self.tickerlist = tickerlist

    def getTickers(self) -> list: 
        print('Screening!') 
        s = Screener()
        data = s.get_screeners('all_cryptocurrencies_us', count=self.count)
        dicts = (data['all_cryptocurrencies_us']['quotes'])
        symbols = [d['symbol'] for d in dicts] + self.read_file() + self.tickerlist
        return symbols
                
    def importData(self,ticker) -> object:
        print('Importing - ', ticker)   
        s = yf.download(ticker,start = self.start, end = self.end, interval=self.freq)
        s[ticker] = s[self.metric]
        return s[ticker]
    
    def DropNullCols(self, df) -> object:
        # Set the threshold for null value, arbitrarily set... 
        threshold = 100
        cols_to_keep = df.isnull().sum()[df.isnull().sum() <= threshold].keys()
        df = df.bfill()
        return df
    def RunExtract(self) -> None: 
        dfs = []
        tickers = self.getTickers()

        #Iterating through all tickers from screener and appending it to dfs vector.
        for ticker in tickers:
            data = self.importData(ticker)
            
            if len(data) > 0:
                dfs.append(data)
   
        #Concatenate the DataFrames along the columns (axis=1) 
        df = pd.concat(dfs, axis=1)
        df = self.DropNullCols(df)
        print('Done!')
        return df
  

    #### STATIC LIST ####
    @staticmethod
    def read_file() -> list:
        with open(r'symbols/symbols.txt', 'r') as f:
            textfile = f.readlines()
            firstrow = textfile[0]
            cols = re.sub(r'\s+', '\t', firstrow).split('\t')
            rows = []
            for i in textfile[1:]:
                raw_row = i.replace('\n', '')
                row = (re.sub(r'\s{2,}', '\t', raw_row)).split('\t') 
                row[1] = row[1].replace(' ', '-') + ".ST"
                rows.append(row)
            df = pd.DataFrame(columns=cols, data=rows)
            return list(df['Symbol'])
        