import requests
from bs4 import BeautifulSoup

url = 'https://fbil.org.in/'
response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')

# Find the "Foreign Exchange" tab link
foreign_exchange_link = soup.find('a', {'class': 'nav-link', 'href': '/ForexDept.html'})

if foreign_exchange_link:
    foreign_exchange_url = url + foreign_exchange_link['href']
    foreign_exchange_response = requests.get(foreign_exchange_url)
    foreign_exchange_soup = BeautifulSoup(foreign_exchange_response.text, 'html.parser')

    # Find the latest INR/USD exchange rate
    forex_table = foreign_exchange_soup.find('table', {'class': 'table'})
    if forex_table:
        rows = forex_table.find_all('tr')
        for row in rows:
            columns = row.find_all('td')
            if len(columns) >= 2 and columns[0].text.strip() == 'INR/1 USD':
                exchange_rate = columns[1].text.strip()
                print("Latest INR/USD exchange rate:", exchange_rate)
                break
else:
    print("Foreign Exchange tab not found on FBIL website.")
