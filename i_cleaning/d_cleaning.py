from sqlalchemy import create_engine
import pandas as pd
import re
from functools import reduce


def clean_data(df):
    print('Cleaning the merged dataframe')
    # cleaning the age column
    df['age'] = df['age'].apply(lambda age: re.sub('[a-z]', '', age))
    df = df.astype({'age': int})
    df['age'] = df['age'].apply(lambda age: age if age < 250 else 2016 - age)
    df['age_group'] = df['age_group'].apply(lambda group: group if group != 'juvenile' else '14_25')

    # cleaning the gender column
    df['gender'] = df['gender'].str.lower()
    df['gender'] = df['gender'].str.replace(r'\b[f]\w+', 'female')
    df['gender'] = df['gender'].str.replace(r'\b[m]\w+', 'male')

    # cleaning the dem_has_children column
    df['dem_has_children'] = df['dem_has_children'].str.lower()
    df['dem_has_children'] = df['dem_has_children'].str.replace(r'\b[y]\w+', 'yes')
    df['dem_has_children'] = df['dem_has_children'].str.replace(r'\b[m]\w+', 'no')

    # cleaning the rural column
    df['rural'] = df['rural'].str.lower()
    df['rural'] = df['rural'].str.replace(r'\b[ci]\w+', 'urban').replace(r'\b[count]\w+', 'no').replace('non-rural', 'rural')

    print('Cleaning finished')
    return df


def col_names(df_clean):
    print('Changing col names')

    df_clean.rename(columns={'age': 'Age (years old)', 'dem_has_children': 'Has Children?', 'age_group': 'Age Group', 'English short name (using title case)': 'Country', 'title': 'Job Title'}, inplace=True)
    df_clean.to_csv(f'/home/juan/IronHack/ih_datamadpt0420_project_m1/data/processed/clean_df.csv')

    print('DF all clean and exported :)')

    return df_clean