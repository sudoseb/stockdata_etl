import yfinance as yf
import datetime
import pandas as pd
df = yf.download('INVE-B.ST',start = '2025-02-01',interval = '15m')
'''
date_str = "2024-02-23 15:30:00+00:00"
date_obj = datetime.fromisoformat(date_str).replace(tzinfo=None)
formatted_date = date_obj.strftime('%Y-%m-%d %H:%M')'''
df = df.reset_index()
df['Datetime'] = pd.to_datetime(df['Datetime']).dt.tz_localize(None).dt.strftime('%Y-%m-%d %H:%M:%S')
print(df['Datetime'])