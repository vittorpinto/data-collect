#%%
import requests
from bs4 import BeautifulSoup
# %%
url = 'https://www.codewars.com/users/vittorpinto'

resp = requests.get(url)

# %%
soup = BeautifulSoup(resp.text)

member_since = soup.find_all('div', class_ ='stat-box mt-1 mb-1 md:mb-0')[1].find('div', class_ = 'stat').get_text()

info, date = member_since.split(':')

date
# %%
