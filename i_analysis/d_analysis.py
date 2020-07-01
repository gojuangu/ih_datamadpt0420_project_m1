import pandas as pd

# analysis functions
def requested_table(df):
    print('Analysing data...')
    df1 = df[df['Job Title'].notna()]
    df2 = df1[['Country', 'Job Title', 'Age Group', 'uuid']]

    df3 = pd.DataFrame(df2.groupby(['Country', 'Job Title', 'Age Group']).size()).reset_index()
    df3 = df3.rename(columns={0: 'Quantity'})
    df3['percetange'] = df3['Quantity'].apply(lambda x: '{:.3%}'.format((x/df3['Quantity'].sum())))

    return df3

def basic_income_cs(df):
    df1 = df[
        df['Basic Income Vote'].isin(['I would vote for it', 'I would vote against it'])]
    countries_resp = pd.DataFrame(
        df1.groupby(['Country', 'Basic Income Vote']).size()).reset_index()
    countries_resp = countries_resp.rename(columns={0: 'Quantity'})
    countries_resp['Percetange'] = countries_resp['Quantity'].apply(
        lambda j: '{:.0%}'.format((j) / countries_resp['Quantity'].sum()))
    return countries_resp

def basic_income_gr(df):
    df1 = df[
        df['Basic Income Vote'].isin(['I would vote for it', 'I would vote against it'])]
    gender_resp = pd.DataFrame(
        df1.groupby(['Gender', 'Basic Income Vote']).size()).reset_index()
    gender_resp = gender_resp.rename(columns={0: 'Quantity'})
    gender_resp['Percetange'] = gender_resp['Quantity'].apply(
        lambda j: '{:.0%}'.format((j) / gender_resp['Quantity'].sum()))
    return gender_resp

def basic_income_ag(df):
    df1 = df[
        df['Basic Income Vote'].isin(['I would vote for it', 'I would vote against it'])]
    age_resp = pd.DataFrame(
        df1.groupby(['Age Group', 'Basic Income Vote']).size()).reset_index()
    age_resp = age_resp.rename(columns={0: 'Quantity'})
    age_resp['Percetange'] = age_resp['Quantity'].apply(
        lambda j: '{:.0%}'.format((j) / age_resp['Quantity'].sum()))
    return age_resp

def basic_income_ar(df):
    df1 = df[
        df['Basic Income Vote'].isin(['I would vote for it', 'I would vote against it'])]
    area_resp = pd.DataFrame(
        df1.groupby(['Area', 'Basic Income Vote']).size()).reset_index()
    area_resp = area_resp.rename(columns={0: 'Quantity'})
    area_resp['Percetange'] = area_resp['Quantity'].apply(
        lambda j: '{:.0%}'.format((j) / area_resp['Quantity'].sum()))
    return area_resp