import pandas as pd
import numpy as np
from  alpaca import Universe

universe = Universe()

prices = universe.get_universe_prices(1000)

prices.index = prices.index.strftime('%Y-%m-%d')

prices.index.name = 'date'

prices

start_date = prices.index[-755]
end_date = prices.index[-1]
df = prices[start_date:end_date]
# df = df.dropna(axis=1, how='any')
df = df[~df.index.duplicated(keep='first')]
df.KRC.dropna().shape





prices.to_csv('ALPACAClosePrices_FULL.csv')
