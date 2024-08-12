#%%
import requests
import pandas as pd
import os
import datetime
import json
import time

print(os.getcwd())
if os.getcwd() != "\JovemNerd":
    os.chdir("../JovemNerd")
print(os.getcwd())

#%%
class Collector():
    def __init__(self, url):
        self.url = url
        
    def get_response(**kwargs):
        url = "https://api.jovemnerd.com.br/wp-json/jovemnerd/v1/nerdcasts/"
        resp = requests.get(url, params=kwargs)
        if resp.status_code == 200:
            return resp
        
    def save_data(self, data, option='json'):
        now = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        
        if option == "json":
            with open (f"./data/episodios/json/{now}.json" , 'w') as open_file:
                json.dump(data, open_file,indent = 4)
        elif option == "parquet":
            df = pd.DataFrame(data)
            df.to_parquet(f".data/episodios/parquet/{now}.parquet", index = False)    

# %%
page = 1
while True:
    date_stop = pd.to_datetime("2023-01-01").date()
    
    if resp.status_code == 200:
        resp = get_response(page=page, per_page=100)
        data = resp.json()
        date = pd.to_datetime(data[-1]["published_at"]).date()
        save_data(data)
        if len(data) < 100 or date < date_stop:
            break
        page += 1
        time.sleep(10)
    else:
        time.sleep(60 * 5)
    
# %%
