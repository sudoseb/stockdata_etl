from _1_stockdata_extract   import StockDataExtraction
from _2_database_insert     import SQL_insert
from _3_database            import Database 
df = Database().df()
import pandas as pd
import matplotlib.pyplot as plt

# Assuming you already have a DataFrame `df` with columns: datetime, ticker, tickertype
# Filter for 'Stock' tickertype (as per your input)
df = df[['ticker','tickertype', 'datetime']]

# Ensure datetime is in datetime format
df['datetime'] = pd.to_datetime(df['datetime'])

# Resample per day and count distinct tickers per tickertype
daily_counts = df.groupby(['tickertype']).resample('D', on='datetime')['ticker'].nunique().reset_index()

# Pivot for better visualization
pivot_df = daily_counts.pivot(index='datetime', columns='tickertype', values='ticker')

# Plot the data
plt.figure(figsize=(12, 6))
pivot_df.plot(kind='line', marker='o', figsize=(12, 6))

plt.title('Daily Count of Unique Tickers per Tickertype')
plt.xlabel('Date')
plt.ylabel('Number of Unique Tickers')
plt.legend(title='Tickertype')
plt.xticks(rotation=45)
plt.grid(True)
plt.show()



