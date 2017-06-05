import quandl
from repoze.lru import lru_cache

_us_equity_eod_vendor = "WIKI/"

class data_client:
    name = "Default data vendor"
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
    ohlc_columns = ["Open","High","Low","Close","Volume"]
    ohlc_adjust_columns = ["Adj. Open","Adj. High","Adj. Low","Adj. Close", "Adj. Volume"]
    columns_map = dict(zip(ohlc_adjust_columns, ohlc_columns))
    
    def __init__(self,vendor_prefix):
        if vendor_prefix == "" or "/" not in vendor_prefix:
            raise ValueError("Vendor prefix '{0}' is invalid".format(vendor_prefix))
        self.vendor = vendor_prefix
    
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
    

quandl_us_eod_client = quandl_client(_us_equity_eod_vendor)

# start_date = "2015-12-31"
# end_date = "2016-12-31"


# price = client.get_daily_adjusted("FB", None, None)
