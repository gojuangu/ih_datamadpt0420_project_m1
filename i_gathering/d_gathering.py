from sqlalchemy import create_engine
import pandas as pd
from functools import reduce
import requests
from bs4 import BeautifulSoup

'''
The below function provide the dataframe (output)with all the info that the database contains
get the info from the provided database (input)
'''
def get_info(path):
    print('Importing the db...')
    sqlitedb_path = path
    engine = create_engine(f'sqlite:///{sqlitedb_path}')

    tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", engine)
    tables_lst = tables['name'].to_list()

    all_dfs = [pd.read_sql_query(f'select * from {i}', engine) for i in tables_lst]
    df_all_tables = reduce(lambda left, right: pd.merge(left, right, on='uuid'), all_dfs)
    df_all_tables.to_csv(f'../ih_datamadpt0420_project_m1/data/raw/data_base_raw.csv')
    return df_all_tables

'''
This function extracts a list of the jobs ids (output) from the dataframe of the above function (input)
'''
def job_ids(df_final):
    jobs_ids = list(df_final['normalized_job_code'].unique())
    print('Import finsihed :)')
    return jobs_ids

'''
With the list of the above function (input) gives you all the info from every job id provided (output)
'''
def get_job(jobs_id):
    print('Calling the api...')
    jobs_name = []
    for id in jobs_id:
        if id == None:
            pass
        else:
            response = requests.get(f'http://api.dataatwork.org/v1/jobs/{id}')
            json_data = response.json()
            jobs_name.append(json_data)
    jobs_df_raw = pd.DataFrame(jobs_name)
    jobs_df = jobs_df_raw.rename(columns={'uuid': "normalized_job_code"})
    jobs_df.to_csv(f'../ih_datamadpt0420_project_m1/data/raw/jobs.csv')
    print('Call finished, data can be found in /data/raw folder :)')
    return jobs_df

def get_country():
    url = 'https://en.wikipedia.org/wiki/ISO_3166-1'
    all_countries_info = pd.read_html(url, header=0)[1]
    needed_cols = ['English short name (using title case)', 'Alpha-2 code']
    countries_df = all_countries_info[needed_cols].rename(columns={'Alpha-2 code': "country_code"})
    countries_df.to_csv(f'../ih_datamadpt0420_project_m1/data/raw/countries.csv')
    return countries_df

#final merge
def all_merged_to_csv(countries_df, jobs_df, df_final):
    df_database = df_final
    first_merge = df_database.merge(countries_df, on='country_code')
    final_merge = first_merge.merge(jobs_df, on='normalized_job_code', how='left')
    final_merge.to_csv(f'../ih_datamadpt0420_project_m1/data/raw/all_data_merged.csv', index=False)
    print('All data merged, find it in processed data folder')
    return final_merge

# web-scraping functions
'''
def get_country(url):
    print('scraping the web...')
    #url = 'https://ec.europa.eu/eurostat/statistics-explained/index.php/Glossary:Country_codes'
    html = requests.get(url).content
    soup = BeautifulSoup(html, 'lxml')
    table = soup.find('table')
    print('Web scraped :)')
    return table
'''