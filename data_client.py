import quandl
from repoze.lru import lru_cache
import pandas_datareader as pdr

class data_client:
    name = "Default data vendor"
    ohlc_columns = ["Open","High","Low","Close","Volume"]
    def get_raw_ts(self,*args):
        raise NotImplementedError
    def get_daily(self, *args):
        raise NotImplementedError
    def get_daily_adjusted(self, *args):
        raise NotImplementedError
    def get_daily_close(self, *args):
        raise NotImplementedError
    def get_daily_close_adjusted(self, *args):
        raise NotImplementedError
    def get_1min_bar(self, *args):
        raise NotImplementedError
        

class quandl_client(data_client):
    name = "Quandl"
    vendor = None
    ohlc_adjust_columns = ["Adj. Open","Adj. High","Adj. Low","Adj. Close", "Adj. Volume"]
    columns_map = None
    
    def __init__(self,vendor_prefix):
        if vendor_prefix == "" or "/" not in vendor_prefix:
            raise ValueError("Vendor prefix '{0}' is invalid".format(vendor_prefix))
        self.vendor = vendor_prefix
        self.columns_map = dict(zip(self.ohlc_adjust_columns, self.ohlc_columns))
    
    @lru_cache(100)
    def get_raw_ts(self, ticker, start = None, end = None):
        quandl_ticker = self.vendor + ticker
        df = quandl.get(quandl_ticker, start_date = start, end_date = end)
        return df
        
    def get_daily(self,ticker, start = None, end = None):
        df = self.get_raw_ts(ticker, start, end)
        ts =df[self.ohlc_columns]
        return ts
        
    def get_daily_adjusted(self, ticker, start = None, end = None):
        df = self.get_raw_ts(ticker, start, end)
        ts = df[self.ohlc_adjust_columns]
        ts.rename(columns = self.columns_map, inplace=True)
        return ts
        
    def get_daily_close(self, ticker, start = None, end = None):
        df = self.get_daily(ticker, start, end)
        return df["Close"]
    
    def get_daily_close_adjusted(self, ticker, start = None, end = None):
        df = self.get_daily_adjusted(ticker, start, end)
        return df["Close"]
    

class yahoo_client(data_client):
    name = "Yahoo"
    ohlc_adjust_columns = ["Adj. Open","Adj. High","Adj. Low","Adj. Close", "Adj. Volume"]
    
    @lru_cache(100)
    def get_raw_ts(self, ticker, start = None, end = None):
        df =  pdr.get_data_yahoo(ticker, start, end)
        return df
        
    def get_daily(self, ticker, start = None, end = None):
        df = self.get_raw_ts(ticker, start, end)
        ts =df[self.ohlc_columns]
        return ts
    
    def get_adj_factor(self, ticker, start = None, end = None):
        df = self.get_raw_ts(ticker, start, end)
        ts = df["Adj Close"] / df["Close"]
        return ts
        
    def get_daily_adjusted(self, ticker, start = None, end = None):
        df = self.get_raw_ts(ticker, start, end)
        adj_factor = self.get_adj_factor(ticker, start, end)
        df = df[self.ohlc_columns]
        ts = df.mul(adj_factor)
        return ts
        
        
_us_equity_eod_vendor = "WIKI/"        
quandl_us_eod_client = quandl_client(_us_equity_eod_vendor)
yahoo_finance_client = yahoo_client()

# start_date = "2015-12-31"
# end_date = "2016-12-31"
# price = quandl_us_eod_client.get_daily_adjusted("FB", None, None)
