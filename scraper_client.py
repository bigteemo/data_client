from repoze.lru import lru_cache
import urllib2
from bs4 import BeautifulSoup

class _scraper_client:
    name = None
    

class _wiki_client(scarper_client):
    name = "Wiki scraper"
    
    class sp500_constituents_scraper(scraper_client):
        SITE = "http://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        
        @lru_cache(1)
        def get_current_constituents(self):
            pass
        
        
    
    @lru_cache(1)
    def get_sp500_constituents(self):
        """
        Get current S&P 500 constituents from Wikipedia
        :return: a dataframe with three columns, Ticker, Company, Sector
        """
        req = urllib2.Request(SITE, headers=hdr)
        page = urllib2.urlopen(req)
        soup = BeautifulSoup(page, "html5lib")  # Only html5lib parser works
        table = soup.find('table', {'class': 'wikitable sortable'})
    
        consts = []
        for row in table.findAll('tr'):
            col = row.findAll('td')
            if len(col) > 0:
                if col[0] is None or col[1] is None or col[2] is None:
                    continue
                ticker = str(col[0].string.strip())
                if col[1].string is None:
                    temp = col[1].findAll('a')[0]
                    company = str(temp.string.strip())
                else:
                    company = str(col[1].string.strip())
                sector = str(col[3].string.strip())
                consts.append([ticker,company,sector])
    
        table = adj_symbol(pd.DataFrame(consts, columns=["ticker","company","sector"]))
        table = table.sort_values("ticker")
        table.set_index(np.arange(len(table)), inplace=True)
        return table
    

class _hsci_client(scraper_client):
    name = "HSCI scraper"
    
    def get_hsci_constituents(self):
        pass
    

# Instantiate client objects
wiki_client = _wiki_client()
hsci_client = _hsci_client()