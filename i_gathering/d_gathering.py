from sqlalchemy import create_engine
import pandas as pd
import re
from functools import reduce
import requests
from bs4 import BeautifulSoup
import lxml.html as lh
import requests


# API functions
def get_job(job_id):
    response = requests.get(f'http://api.dataatwork.org/v1/jobs/{job_id}')
    json_data = response.json()
    return json_data


# database functions
def get_info(sql):
    sqlitedb_path = sql
    engine = create_engine(f'sqlite:///{sqlitedb_path}')

    tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", engine)
    tables_lst = tables['name'].to_list()

    dfs = [pd.read_sql_query(f'select * from {i}', engine) for i in tables_lst]
    df_final = reduce(lambda left, right: pd.merge(left, right, on='uuid'), dfs)
    return df_final


# web-scraping functions
def get_country(url):
    #url = 'https://ec.europa.eu/eurostat/statistics-explained/index.php/Glossary:Country_codes'
    html = requests.get(url).content
    soup = BeautifulSoup(html, 'lxml')
    table = soup.find('table')
    return table