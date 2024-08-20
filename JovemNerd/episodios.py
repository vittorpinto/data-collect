#%%
import requests
import pandas as pd
import os
import datetime
import json
import time

print(os.getcwd())
if os.getcwd() != "/JovemNerd":
    os.chdir("../JovemNerd")
print(os.getcwd())

#%%
class Collector():
    def __init__(self, url, instance_name):
        self.url = url
        self.instance_name = instance_name
    
    def get_response(self, **kwargs):
        resp = requests.get(self.url, params=kwargs)
        return resp
        
    def save_parquet(self, data):
        now = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        df = pd.DataFrame(data)
        df.to_parquet(f".data/{self.instance_name}/parquet/{now}.parquet", index = False)
           
    def save_json(self, data):
        now = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        with open (f"./data/{self.instance_name}/json/{now}.json" , 'w') as open_file:
            json.dump(data, open_file,indent = 4)
        
    def save_data(self, data, option='json'):
        if option == "json":
            self.save_json(data)
        elif option == "parquet":
            self.save_parquet(data)               

    def get_and_save(self, save_format = 'json', **kwargs):
        resp = self.get_response(**kwargs)
        if resp.status_code == 200:
            data = resp.json()
            self.save_data(data, save_format)
        else:
            data = None
            print(f'Request sem sucesso! Error code: {resp.status_code}') 
        return data
            
    def auto_exec(self, save_format = 'json', date_stop = '2000-01-01'):
        page = 1
        date_stop = pd.to_datetime(date_stop).date()
        while True:
            print(page)
            data = self.get_and_save(save_format=save_format, page=page,per_page = 1000)
            if data == None:
                print('Erro ao coletar dados...')
                time.sleep(60*5)
            else: 
                last_date = pd.to_datetime(data[-1]["published_at"]).date()
                if last_date < pd.to_datetime(date_stop).date():
                    break
                elif len(data) < 1000:
                    break
                page+=1
                time.sleep(10)

# %%
url = "https://api.jovemnerd.com.br/wp-json/jovemnerd/v1/nerdcasts/"

collect = Collector(url, "episodios")
    
# %%
collect.auto_exec()
 # %%

