class Signal:

    def __init__(self,Data):
        self.Data = Data

    def create_signal_dataframe(self):

        import numpy as np
        import pandas as pd

        bars_data = self.Data.create_dataframe('minute')
        interpolated_data = self.Data.create_dataframe('interpolated')

        df = bars_data.merge(interpolated_data,left_index=True,right_index=True)

        df['above'] = np.where(df['open'] > df['line'],1,0)
        df['below'] = np.where(df['open'] < df['line'],1,0)

        df['above_count'] = df['above'].cumsum()
        df['above_count'] = df['above_count'].sub(df['above_count'].mask(df['above']!= 0).ffill(), fill_value=0).astype(int)
        df['long_signal'] = np.where(df['above_count'] == 4,-1,0)


        df['below_count'] = df['below'].cumsum()
        df['below_count'] = df['below_count'].sub(df['below_count'].mask(df['below']!= 0).ffill(), fill_value=0).astype(int)
        df['short_signal'] = np.where(df['below_count'] == 4,1,0)

        df['signal'] = df['long_signal'] + df['short_signal']

        this_morning = df.head(1)

        # if we are looking to get long
        if this_morning['open'].iloc[0] < this_morning['line'].iloc[0]:

            #find first signal value, and ignore it if it's short
            first_signal = df[~(df.signal == 0)].iloc[0]['signal']
            first_time = df[~(df.signal == 0)].index[0]

            if first_signal > 0:
                df['{}'.format(first_time):'{}'.format(first_time)]['signal'] = 0

        #else if we are looking to get short
        else:
            #find first signal value, and ignore it if it's long
            first_signal = df[~(df.signal == 0)].iloc[0]['signal']
            first_time = df[~(df.signal == 0)].index[0]

            if first_signal < 0:
                df['{}'.format(first_time):'{}'.format(first_time)]['signal'] = 0



        return df