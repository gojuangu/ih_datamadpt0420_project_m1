
import pandas as pd

# list of the european countries in a dataframe
def countries_all(table):
    countries = []

    rows = table.find_all('tr')

    for tr in rows:
        cols = tr.find_all('td')
        for td in cols:
            countries.append(td.text)

    countries_list = [i.replace('\n', '').replace('(', '').replace(')', '') for i in countries]
    two_cols = [countries_list[i:i + 2] for i in range(0, len(countries_list), 2)]
    countries_df = pd.DataFrame(two_cols)

    return countries_df

# list of the jobs in a dataframe
def jobs_all(jobs_id):
    jobs_name = []
    for job in jobs_id:
        job_name = get_job(job)
        jobs_name.append(job_name)
    jobs_df = pd.DataFrame(jobs_name)
    return jobs_df

#dataframe of the database without none as job
def drop_na_jobs(get_info):
    col_to_clean = 'normalized_job_code'
    df_clean = get_info[get_info[col_to_clean].notna()]
    return df_clean

#final merge
def all_merged_to_csv(countries_df, jobs_df, df_clean):
    countries_df