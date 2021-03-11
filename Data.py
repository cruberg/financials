class Data:

    def __init__(self,alpaca_api_key,alpaca_api_secret,symbol,ts):
        import alpaca_trade_api as tradeapi
        import datetime

        self.alpaca_api_key = alpaca_api_key
        self.alpaca_api_secret = alpaca_api_secret

        self.ts = ts
        self.symbol = symbol

        self.api = tradeapi.REST(
            self.alpaca_api_key,
            self.alpaca_api_secret,
            'https://paper-api.alpaca.markets',
            api_version='v2'
        )

    def get_minute_bars(self):
        import pandas as pd
        ts = pd.Timestamp('{}'.format(self.ts), tz='America/New_York')
        start = ts.replace(hour=9, minute=30).astimezone('GMT').isoformat()[:-6]+'Z'
        end = ts.replace(hour=16, minute=1).astimezone('GMT').isoformat()[:-6]+'Z'
        df = self.api.get_barset(self.symbol, '1Min', start=start, end=end).df
        df.columns = df.columns.droplevel()
        return df[['open']]

    def get_daily_bars(self):
        import pandas as pd
        ts = pd.Timestamp('{}'.format(self.ts), tz='America/New_York')
        e = ts + pd.DateOffset(days=1)
        e = e.replace(hour=9, minute=30).astimezone('GMT').isoformat()[:-6]+'Z'
        df = self.api.get_barset(self.symbol, 'day',until=e,limit=252).df
        df.columns = df.columns.droplevel()
        return df[['open']]

    def calc_mvg_avg(self,length=9,obs=10):

        daily = self.get_daily_bars()

        return daily.rolling(length).mean().tail(obs)
    
    def calc_slope(self,y1,y2,periods=1440):
        
        return (y2 - y1) / periods 

    def interpolate(self,end,slope):
    
        outputs = list(range(1,1441))

        outputs[0] = end * 1.00

        for i in range(1, 1440):

            outputs[i] = outputs[0] + (slope * i)
            
        return outputs

    def create_dataframe(self,datatype):

        import numpy as np
        import pandas as pd
        import pandas as pd
        pd.options.mode.chained_assignment = None
        import datetime

        if datatype in ['minute'.upper(),'minute'.lower(),'minute'.capitalize()]:
            df = self.get_minute_bars()
            return df

        if datatype in ['day'.upper(),'day'.lower(),'day'.capitalize(),'daily'.upper(),'daily'.lower(),'daily'.capitalize()]:
            df = self.get_daily_bars()
            return df

        if datatype in ['interpolate','interpolated'.lower(),'interpolated'.capitalize(),'interpolated'.upper()]:

            mas = self.calc_mvg_avg()

            start = mas['open'].tail(2).iloc[0]
            end = mas['open'].tail(2).iloc[1]

            s = self.calc_slope(start,end)

            line = self.interpolate(end,s)
            interpolation = pd.DataFrame(line)
            interpolation.columns = ['line']

            ts = pd.Timestamp('{}'.format(self.ts), tz='America/New_York')
            ts = ts.replace(hour=9, minute=30)
            start = ts.replace(hour=9, minute=30).astimezone('GMT').isoformat()[:-6]+'Z'
            end = ts.replace(hour=16, minute=1).astimezone('GMT').isoformat()[:-6]+'Z'

            interpolation['t'] = ts

            for i in range(0,len(interpolation)):
    
                prev = i - 1
            
                if i > 0:
                    interpolation['t'].iloc[i] = interpolation['t'].iloc[prev] + pd.DateOffset(minutes=1)
                
            interpolation = interpolation.set_index('t')
            interpolation.index.name = 'time'


            return interpolation