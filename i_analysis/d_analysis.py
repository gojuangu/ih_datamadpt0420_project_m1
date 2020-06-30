import pandas as pd
from pandas import ExcelWriter
# analysis functions
def requested_table(df, country):

    df1 = df[df['Job Title'].notna()]
    df2 = df1[['Country', 'Job Title', 'Age Group', 'uuid']]

    df3 = pd.DataFrame(df2.groupby(['Country', 'Job Title', 'Age Group']).size()).reset_index()
    df3 = df3.rename(columns={0: 'counts'})
    df3['percetange'] = df3['counts'].apply(lambda x: '{:.3%}'.format((x/df3['counts'].sum())))

    if country is not None:
        df4 = df3[df3['Country'] == f'{country}']

        df4['percetange'] = df4['counts'].apply(lambda x: '{:.3%}'.format((x / df4['counts'].sum())))
        #df4.to_csv(f'/home/juan/IronHack/ih_datamadpt0420_project_m1/data/results/{country}_prueba.csv')
        #print(f'DF of {country} analysed!')
        writer = ExcelWriter(f'/home/juan/IronHack/ih_datamadpt0420_project_m1/data/results/pruebaj.xlsx')
        frames = {'All Data': df3, f'{country} data': df4}
        for sheet, frame in frames.items():
            frame.to_excel(writer, sheet_name= sheet, index=False)
        writer.save()
    else:
        df3.to_csv(f'/home/juan/IronHack/ih_datamadpt0420_project_m1/data/results/prueb.csv')