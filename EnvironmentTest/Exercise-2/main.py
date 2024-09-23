#%%
import requests
import pandas
from bs4 import BeautifulSoup
import datetime
import csv
import os

#%%
print(os.getcwd())
# os.chdir('./EnvironmentTest/Exercise-2')
#%%
url = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/?C=M;O=A'
resp = requests.get(url)
# %%
soup = BeautifulSoup(resp.text) 

#%%
data = []
table = soup.find('table')
rows = table.find_all('tr')[3:-1]
#%%
for row in rows:
    temp_arr = []
    td_elements = row.find_all('td')
    for td in td_elements[:2]:
        temp_arr.append(td.get_text().strip())
    data.append(temp_arr)

if len(data)>0:
    temp_date = datetime.datetime.strptime(data[0][1],"%Y-%m-%d %H:%M")
else:
    temp_date = None

for i in range(0,len(data)-2):
    date = datetime.datetime.strptime(data[i][1],"%Y-%m-%d %H:%M")
    date_next = datetime.datetime.strptime(data[i+1][1],"%Y-%m-%d %H:%M")
    if date <= date_next:
        temp_arr = data[i+1]
temp_arr 
# %%
file_name = temp_arr[0]
url_raw = '/'.join(url.split('/')[:-1])
url_to_download = f'{url_raw}/{file_name}'
url_to_download
# %%
resp_url_to_download = requests.get(url_to_download)

# %%
with open('./data/last-file.csv', 'w') as file:
    writer = csv.writer(file)
    for line in resp_url_to_download.iter_lines():
        writer.writerow(line.decode('utf-8').split(','))
# %%
