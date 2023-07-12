import requests
from bs4 import BeautifulSoup

# Send a GET request to the website
url = 'https://www.fbil.org.in/'
response = requests.get(url)

# Parse the HTML content
soup = BeautifulSoup(response.content, 'html.parser')

# Find the "Foreign Exchange" button and get its URL
foreign_exchange_button = soup.find('a', text='Foreign Exchange')
foreign_exchange_url = foreign_exchange_button['href']

# Send a GET request to the Foreign Exchange page
response_fx = requests.get(foreign_exchange_url)
fx_soup = BeautifulSoup(response_fx.content, 'html.parser')

# Find the latest INR/USD rate
latest_rate_element = fx_soup.find('td', text='INR / 1 USD')
rate_value = latest_rate_element.find_next_sibling('td').text

# Print the rate
print(rate_value)
