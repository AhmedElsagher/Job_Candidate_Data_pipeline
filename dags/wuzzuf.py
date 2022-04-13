from enum import IntEnum
import json 
import pathlib 
import datetime
import re
import copy
import glob
import requests
import os
from bs4 import BeautifulSoup
import pandas as pd
import airflow 
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


dag = DAG(dag_id="wuzzuf",
                start_date=datetime.datetime(2019,1,1),
                schedule_interval=None)

def _get_profiles_urls(start_url,url_txt_path,**context):
    # URL
    data ={"profile":[]}
    url=start_url
    count = 0
    while True:
        page = requests.get(url)
        soup = BeautifulSoup(page.content,'html.parser')
        divs = soup.find_all('div',{"class":"col-md-6"})
        for div in divs:
            url = div.find("a").get("href")
            data["profile"].append(url)
        if  soup.find('a',{"aria-label":"Next"}):
            url =  soup.find('a',{"aria-label":"Next"}).get("href")
        else:
            break
        count+=1
        if count == 1:break
    with open(url_txt_path,"w") as file_:
        file_.writelines("\n".join( data["profile"]))
        

get_profiles_urls = PythonOperator(
    task_id="get_profiles_urls",
    python_callable=_get_profiles_urls,
    op_kwargs={'url_txt_path': "urls.txt",
                'start_url': "https://wuzzuf.net/directory/members/a?p=1"},
    dag=dag
)

def _download_json(url_txt_path,output_folder,**context):
    urls = open(url_txt_path,"r").read()
    urls = urls.split()
    for i,url  in enumerate(urls):
        page = requests.get(url)
        soup = BeautifulSoup(page.content,'html.parser')
        script = soup.find("script")
        profile = script.text.split("\n")[4].split("initialStoreState = ")[1][:-1]
        profile = json.loads(profile)
        profile = profile['publicProfile']
        with open(f"{output_folder}/{url.split('/')[-1]}.json","w", encoding='utf8')as file:
            file.write(json.dumps(profile,indent=4,ensure_ascii=False))
        if i==10:
            break

download_json = PythonOperator(
    task_id="download_json",
    python_callable=_download_json,
    op_kwargs={'url_txt_path': "urls.txt",
                'output_folder': "/home/ahmed/projects/sideprojects/wuzzuf/profiles"},
    dag=dag
)




def _parse_json(input_folder,output_folder,**context):

    files = glob.glob(f"{input_folder}/*.json")
    print("-----------------------77777777777-----------", files)
    for json_path in files:
        profile = json.loads(open(json_path,"r").read())
        data= {}
        print(profile.keys())
        # profile=profileprofile['talent']['data']
        personal_data = profile['talent']['data']['user']['undefined']['attributes']
        data.update(personal_data)
        if profile['talent']['data'].get('talentSkill'):
            skills = [profile['talent']['data']['talentSkill'][i]['attributes'] for i in profile['talent']['data']['talentSkill'].keys()]
            skills={"skills":skills }
            data.update(skills)
        area,city,country = None,None,None
        if profile['talent']['data'].get('area'):
            area =list(profile['talent']['data']['area'].values())[0]['attributes']['name']
        if profile['talent']['data'].get('city'):
            area =list(profile['talent']['data']['city'].values())[0]['attributes']['name']
        if profile['talent']['data'].get('country'):
            area =list(profile['talent']['data']['country'].values())[0]['attributes']['name']
        address = {"address":{"area":area,"city":city,"country":country} }
        data.update(address)
        if profile['talent']['data'].get('userOnlinePresence'):
            userOnlinePresence = [ profile['talent']['data']['userOnlinePresence'][i]['attributes'] for i in profile['talent']['data']['userOnlinePresence']]
            userOnlinePresence_keys =[str.lower(re.findall(r"//(.+)\.",i['link'])[0])  for i in userOnlinePresence]
            userOnlinePresence_keys =[ i.split(".")[-1]  for i in userOnlinePresence_keys]
            userOnlinePresence = {userOnlinePresence_keys[i]:userOnlinePresence[i]['link'] for i in range(len(userOnlinePresence))}
            data.update(userOnlinePresence)
        print(data)
        print(f"{output_folder}/{os.path.basename(json_path)}")
        with open(f"{output_folder}/{os.path.basename(json_path)}","w", encoding='utf8')as file:
            file.write(json.dumps(data,indent=4,ensure_ascii=False))



parse_json = PythonOperator(
    task_id="parse_json",
    python_callable=_parse_json,
    op_kwargs={"input_folder": "/home/ahmed/projects/sideprojects/wuzzuf/profiles",
                'output_folder': "/home/ahmed/projects/sideprojects/wuzzuf/processed_profiles"},
    dag=dag
)


get_profiles_urls >> download_json >>parse_json