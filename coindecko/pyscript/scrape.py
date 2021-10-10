import requests
from bs4 import BeautifulSoup

url = 'https://www.coingecko.com/en/coins/all'

soup = BeautifulSoup(requests.get(url).content, 'html.parser')

table_cd = soup.find_all(class_="td")
print(table_cd.prettify())