from pandas import ExcelWriter

# reporting functions

def to_excel(df_f, countries, df_cs, df_gr, df_ag, df_ar):
    print('Exporting data')
    if countries is not None:
        df_c = df_f[df_f['Country'].isin(countries)]
        df_c['percetange'] = df_c['Quantity'].apply(lambda x: '{:.3%}'.format((x / df_c['Quantity'].sum())))

        writer = ExcelWriter(f'/home/juan/IronHack/ih_datamadpt0420_project_m1/data/results/final_data_countries.xlsx')
        frames = {'All Data': df_f,
                  'Countries data': df_c,
                  'BI info by countries': df_cs,
                  'BI info by gender': df_gr,
                  'BI info by age group': df_ag,
                  'BI info by area': df_ar}

        for sheet, frame in frames.items():
            frame.to_excel(writer, sheet_name= sheet, index=False)
        writer.save()
    else:
        writer = ExcelWriter(f'/home/juan/IronHack/ih_datamadpt0420_project_m1/data/results/final_data.xlsx')
        frames = {'All Data': df_f,
                  'BI info by countries': df_cs,
                  'BI info by gender': df_gr,
                  'BI info by age group': df_ag,
                  'BI info by area': df_ar}

        for sheet, frame in frames.items():
            frame.to_excel(writer, sheet_name=sheet, index=False)
        writer.save()