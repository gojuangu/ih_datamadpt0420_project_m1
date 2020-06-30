import pandas as pd

# analysis functions
def analysis_data(df_1, country):
    df = df_1[df_1['Job Title'].notna()]
    df_2 = df[['Country', 'Job Title', 'Age Group', 'uuid']]
    df_3 = df_2[df_2['Country'] == f'{country}']
    #k = df_2.groupby(['English short name (using title case)', 'title','age_group']).size()
    df_3.to_csv(f'/home/juan/IronHack/ih_datamadpt0420_project_m1/data/results/prueba.csv')

