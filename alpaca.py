class Universe(object):

    def __init__(self):
        import alpaca_trade_api as tradeapi
        import datetime
        from charlie import my_alpaca_key, my_alpaca_secret
        import pandas as pd
        import numpy as np

        self.alpaca_api_key = my_alpaca_key
        self.alpaca_api_secret = my_alpaca_secret

        self.api = tradeapi.REST(
            self.alpaca_api_key,
            self.alpaca_api_secret,
            'https://paper-api.alpaca.markets',
            api_version='v2'
        )

    # Create a function called "chunks" with two arguments, l and n:
    def get_chunks(self,l, n):
        # For item i in a range that is a length of l,
        for i in range(0, len(l), n):
            # Create an index range for l of n items:
            yield l[i:i+n]

    def get_active_universe(self):

        # Get a list of all active assets.
        self.active_assets = self.api.list_assets(status='active')

        # Filter the assets down to just those on NYSE & NASDAQ.
        self.active_assets = [a for a in self.active_assets if a.symbol.__contains__('-') == False]
        self.active_assets = [a for a in self.active_assets if a.symbol.__contains__('.') == False]
        # self.active_assets = [a for a in self.active_assets if a.easy_to_borrow == True]
        self.active_assets = [a.symbol for a in self.active_assets if a.exchange in ['NYSE','NASDAQ']]
        # self.active_assets = [a.replace('-', 'p') for a in self.active_assets]
        # self.active_assets = [a.replace('.', '/') for a in self.active_assets]

        return sorted(self.active_assets)

    def get_minute_bars(self,symbol,ts):
        import pandas as pd
        ts = pd.Timestamp('{}'.format(self.ts), tz='America/New_York')
        start = ts.replace(hour=9, minute=30).astimezone('GMT').isoformat()[:-6]+'Z'
        end = ts.replace(hour=16, minute=1).astimezone('GMT').isoformat()[:-6]+'Z'
        df = self.api.get_barset(symbol, '1Min', start=start, end=end).df
        df.columns = df.columns.droplevel()
        return df[['open']]

    def get_daily_bars(self,symbol,ts):
        import pandas as pd
        ts = pd.Timestamp('{}'.format(self.ts), tz='America/New_York')
        e = ts + pd.DateOffset(days=1)
        e = e.replace(hour=9, minute=30).astimezone('GMT').isoformat()[:-6]+'Z'
        df = self.api.get_barset(symbol, 'day',until=e,limit=252).df
        df.columns = df.columns.droplevel()
        return df[['open']]
        
    def get_universe_prices(self,lookback):
        import pandas as pd
        _prices = pd.DataFrame()

        symbols = self.get_active_universe()
        chunks = self.get_chunks(symbols,200)

        num = 0

        for c in sorted(chunks):

            data = self.api.get_barset(c, 'day', limit = lookback).df
            data = data.iloc[:, data.columns.get_level_values(1)=='close'].swaplevel(axis=1)
            data.columns = data.columns.droplevel()
            data = data.ffill()

            if num == 0:
                _prices = data
            else:
                _prices = _prices.merge(data,left_index=True,right_index=True,how='outer')

            num += 1

        return _prices



    