import pandas as pd
import io
import requests
import os
import numpy as np
import warnings
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings("ignore")
import yfinance as yf

class Universe(object):

    def __init__(self):
        import os
        from config import CONSUMER_KEY, REDIRECT_URI, JSON_PATH
        from charlie import TD_ACCOUNT, my_Alpaca_key, my_Alpaca_secret, filepath, outpath
        from td.client import TDClient
        import alpaca_trade_api as tradeapi
        from datetime import datetime
        from datetime import timedelta
        import pandas as pd
        import numpy as np
        import time
        os.chdir(filepath)

        self.TDSession = TDClient(client_id = CONSUMER_KEY, redirect_uri = REDIRECT_URI, credentials_path = JSON_PATH)
        self.td_client = TDClient(CONSUMER_KEY, REDIRECT_URI, TD_ACCOUNT, JSON_PATH)
        self.td_client.login()

        self.alpaca_api_key = my_Alpaca_key
        self.alpaca_api_secret = my_Alpaca_secret

        self.api = tradeapi.REST(
            self.alpaca_api_key,
            self.alpaca_api_secret,
            'https://paper-api.alpaca.markets',
            api_version='v2'
        )

    def get_active_universe(self):

        # Get a list of all active assets.
        self.active_assets = self.api.list_assets(status='active')

        # Filter the assets down to just those on NASDAQ.
        self.active_assets = [a for a in self.active_assets if a.symbol.__contains__('-') == False]
        self.active_assets = [a for a in self.active_assets if a.symbol.__contains__('.') == False]
        # self.active_assets = [a for a in self.active_assets if a.easy_to_borrow == True]
        self.active_assets = [a.symbol for a in self.active_assets if a.exchange in ['NYSE','NASDAQ']]
        # self.active_assets = [a.replace('-', 'p') for a in self.active_assets]
        # self.active_assets = [a.replace('.', '/') for a in self.active_assets]

        return sorted(self.active_assets)

    def get_active_symbols(self):

        # Get a list of all active assets.
        self.active_assets = self.api.list_assets(status='active')

        # Filter the assets down to just those on NASDAQ.
        self.active_assets = [a for a in self.active_assets if a.symbol.__contains__('-') == False]
        self.active_assets = [a for a in self.active_assets if a.symbol.__contains__('.') == False]
        # self.active_assets = [a for a in self.active_assets if a.easy_to_borrow == True]
        self.active_assets = [a.symbol for a in self.active_assets if a.exchange in ['NYSE','NASDAQ']]
        # self.active_assets = [a.replace('-', 'p') for a in self.active_assets]
        # self.active_assets = [a.replace('.', '/') for a in self.active_assets]

        return pd.DataFrame(sorted(self.active_assets),columns=['Symbol'])

    def get_info(self,symbol):
        import finviz
        out = {}
        try:
            time.sleep(0.2)
            f = finviz.get_stock(symbol)
            out['Symbol'] = symbol
            out['Sector'] = f['Sector']
            out['Industry'] = f['Industry']
        
        except:
            print("{} Failed".format(symbol))

        return out

    def get_sector_table_full(self):
        symbols = self.get_active_universe()

        print("Fetching Sectors for {} active symbols".format(len(symbols)))

        out = [self.get_info(s) for s in symbols]
        out = pd.DataFrame(out)
        out['FetchDate'] = datetime.today().strftime('%Y-%m-%d')
        out.to_csv(os.path.join(outpath,'SymbolInfo_Full.csv'))

        return out

    def get_sector_table_update(self):
        existing = pd.read_csv(os.path.join(outpath,'SymbolInfo_Full.csv'),engine='c')
        existing = existing.columns.tolist()
        latest = self.get_active_universe() 
        new = sorted(latest - existing)
        print("Fetching Sectors for {} new symbols".format(len(new)))

        update = [self.get_info(s) for s in new]
        update = pd.DataFrame(update)
        update['FetchDate'] = datetime.today().strftime('%Y-%m-%d')
        out = existing.append(update)
        out.to_csv(os.path.join(outpath,'SymbolInfo_New_{}.csv'.format(datetime.date.today().strftime("%Y-%m-%d"))))
        return out

    def get_daily_prices_full(self):

        symbols = self.get_active_universe()
        _prices = pd.DataFrame()

        print("FETCHING PRICES FOR {} ACTIVE SYMBOLS".format(len(symbols)))

        for s in sorted(symbols):
            time.sleep(0.25)
            
            try: 
                p = self.TDSession.get_price_history(
                    symbol = s,
                    period_type = 'year',
                    period = 4,
                    frequency_type = 'daily',
                    frequency = 1,
                    extended_hours=False
                )

                p1 = pd.DataFrame(p)
                p1 = p1.candles.apply(pd.Series)
                p1['timestamp'] = p1.datetime / 1000
                p1['date'] = p1.timestamp.apply(lambda x: datetime.utcfromtimestamp(x).date())
                p1.drop(columns=['datetime','timestamp'],inplace=True)
                p1 = p1.set_index('date')
                p1 = p1[['close']]
                p1.rename(columns={'close' : '{}'.format(s)}, inplace=True)

                if s == sorted(symbols)[0]:
                    _prices = p1
                else:
                    _prices = _prices.merge(p1,left_index=True,right_index=True,how='outer')
            except:
                print("Symbol Failed: {}".format(s))

        _prices = _prices.drop_duplicates()
        _prices.to_csv(os.path.join(outpath,'TDClosePrices_FULL.csv'))

        return _prices

    def get_daily_prices_update(self):

        symbols = self.get_active_universe()
        existing_prices = pd.read_csv(out.to_csv(os.path.join(outpath,'TDClosePrices_FULL.csv',index_col='date',engine='c')

        print("UPDATING PRICES FOR {} ACTIVE SYMBOLS".format(len(symbols)))

        for s in sorted(symbols):
            time.sleep(0.2)
            
            try: 
                p = self.TDSession.get_price_history(
                    symbol = s,
                    period_type = 'month',
                    period = 1,
                    frequency_type = 'daily',
                    frequency = 1,
                    extended_hours=False
                )

                p1 = pd.DataFrame(p)
                p1 = p1.candles.apply(pd.Series)
                p1['timestamp'] = p1.datetime / 1000
                p1['date'] = p1.timestamp.apply(lambda x: datetime.utcfromtimestamp(x).date())
                p1.drop(columns=['datetime','timestamp'],inplace=True)
                p1 = p1.set_index('date')
                p1 = p1[['close']]
                p1.rename(columns={'close' : '{}'.format(s)}, inplace=True)

                if s == sorted(symbols)[0]:
                    _prices = p1
                else:
                    _prices = _prices.merge(p1,left_index=True,right_index=True,how='outer')
            except:
                print("Symbol Failed: {}".format(s))

        #### get dataframe of just new symbols
        new_symbols = symbols - existing_prices.columns.values.tolist()


        _prices = _prices.drop_duplicates()
        _prices.to_csv(out.to_csv(os.path.join(outpath,'TDClosePrices_DELTA_{}.csv'.format(datetime.today().strftime('%Y-%m-%d')))

        return _prices
