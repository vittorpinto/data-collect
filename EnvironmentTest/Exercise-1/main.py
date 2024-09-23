#%%
import urllib.request
import os
from tqdm import tqdm
import zipfile
import aiohttp
import asyncio
import nest_asyncio


#%%
nest_asyncio.apply()
    
#%%
download_path = './downloads/'

os.makedirs('downloads', exist_ok=True)

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

#%%
def normal_extract(download_uris):
    
    for uri in tqdm(download_uris):
        try:
            fileName = uri.split('/')[-1]
            file = os.path.join(download_path,fileName)
            
            urllib.request.urlretrieve(uri, file)
            print(f'Download do arquivo {fileName} concluído')
        
            with zipfile.ZipFile(file, 'r') as zip_ref:
                zip_ref.extractall(download_path)
            
            os.remove(file)
            print (f'Deleted zip file {file}')
                
        except Exception:
            print(f'Não foi possível realizar o download do arquivo {fileName}')

#%%
async def download_and_extract(session, uri):
    try:
        fileName = uri.split('/')[-1]
        file = os.path.join(download_path, fileName)
        
        async with session.get(uri) as response:
            if response.status == 200:
                with open(file, 'wb') as f:
                    f.write(await response.read())
                print (f'Download do arquivo {fileName} concluido com sucesso.')
        
                with zipfile.ZipFile(file, 'r') as zip_ref:
                    zip_ref.extractall(download_path)
                print (f'Arquivo {fileName} extraído com sucesso.')
                
                os.remove(file)
                print (f'Arquivo {fileName} excluído com sucesso.')
            
            else:
                print(f'Falha no download de {fileName}, decorrido ao erro {response.status}')
                
    except Exception as e:
        print(f'Alguma coisa deu errado chefia: {e}')
        
        
# %%
async def async_extract(download_uris):
    async with aiohttp.ClientSession() as session:
        tasks = [download_and_extract(session,uri) for uri in download_uris]
        for f in tqdm(asyncio.as_completed(tasks), total = len(tasks)):
            await f

#%%

await async_extract(download_uris)

#%%
# normal_extract(download_uris)