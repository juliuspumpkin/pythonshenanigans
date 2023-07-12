import requests
from bs4 import BeautifulSoup

# Send a GET request to the FBIL page
response = requests.get('https://www.fbil.org.in')

# Parse the HTML content of the response
soup = BeautifulSoup(response.content, 'html.parser')

# Find the element with the text "FOREIGN EXCHANGE"
foreign_exchange_element = soup.find('b', text='FOREIGN EXCHANGE')

# Get the parent <a> tag
a_tag = foreign_exchange_element.parent

# Extract the href attribute value
href = a_tag['href']

# Construct the complete URL for the FOREIGN EXCHANGE page
foreign_exchange_url = f'https://www.fbil.org.in{href}'

# Send a GET request to the FOREIGN EXCHANGE page
foreign_exchange_response = requests.get(foreign_exchange_url)

# Parse the HTML content of the FOREIGN EXCHANGE response
foreign_exchange_soup = BeautifulSoup(foreign_exchange_response.content, 'html.parser')

# Find the rate for INR / 1 USD
rate_element = foreign_exchange_soup.find('td', text='INR / 1 USD')

if rate_element:
    # Get the rate value from the next sibling element
    rate_value = rate_element.find_next_sibling('td').text.strip()

    print(f"The rate for INR / 1 USD is: {rate_value}")
else:
    print("Rate not found")
