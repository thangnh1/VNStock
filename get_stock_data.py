# Get all stock data and save to data.csv

from vnstock import listing_companies, stock_historical_data, last_xd
from datetime import datetime
import pandas as pd

ticker = listing_companies()['ticker'].tolist()

today = datetime.today().strftime('%Y-%m-%d')

data = pd.DataFrame()
index = 0
for item in ticker:
    index+=1
    print('Process item {} : {}'.format(index,item))
    try:
        item_data = stock_historical_data(item,last_xd(365),today)
    except:
        continue
    item_data['Ticker'] = item
    item_data = item_data.set_index('Open')
    data = pd.concat([data, item_data])

data.to_csv('data.csv', header=False)