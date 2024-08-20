#%%
import requests
import json
import pandas
import datetime
import os

print (os.getcwd())
os.chdir("../pokemon")
# %%
class Collector():
    
    def __init__(self,url):
        self.url = url
        self.instance_name = self.url.strip("/").split("/")[-1]
        
    def get_endpoint(self, **kwargs):
        resp = requests.get(self.url, params = kwargs)
        return resp
    
    def save_data(self, data):
        now = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S-%f")
        filename = f"./data/{self.instance_name}/{now}.json"
        data["ingestion_date"] = datetime.datetime.now().strftime("%Y-%m-%d %H-%M-%S")
        with open (filename, 'w') as (open_file):
            json.dump(data,open_file)
            
    def get_and_save(self, **kwargs):
        resp = self.get_endpoint(**kwargs)
        if resp.status_code == 200:
            data = resp.json()
            self.save_data(data)
            return data
        else:
            return {}
        
    def auto_exec(self,limit = 50):
        offset = 0
        while True:
            data = self.get_and_save(limit=limit, offset=offset)
            if data["next"] == None:
                break
            offset += limit
# %%
url = "https://pokeapi.co/api/v2/pokemon"

collect = Collector(url)

collect.auto_exec()
# %%
