import argparse
from i_gathering import d_gathering as dga
from i_cleaning import d_cleaning as dcl
from i_analysis import d_analysis as dan


def argument_parser():
    parser = argparse.ArgumentParser(description='specify inputs')
    parser.add_argument('-u', "--url", type=str, help='specify path web scraping', required=True)
    parser.add_argument('-p', "--path", type=str, help='specify database', required=True)
    parser.add_argument('-c', "--country", type=str, help='specify database', required=True)
    args = parser.parse_args()

    return args


def main(arguments):
    print('Starting the pipeline...')
    #first part - to gather all the info
    df_all_tables = dga.get_info(arguments.path)
    countries_df = dga.get_country(arguments.url)
    jobs_ids = dga.job_ids(df_all_tables)
    jobs_df = dga.get_job(jobs_ids)
    merged_data = dga.all_merged_to_csv(countries_df, jobs_df, df_all_tables)

    #second part - to clean all the data
    clean_df = dcl.clean_data(merged_data)
    export_clean = dcl.col_names(clean_df)

    #third part - to analyse the data
    one = dan.analysis_data(export_clean,arguments.country)
    print('The pipeline has finished...')


if __name__ == '__main__':
    arguments = argument_parser()
    main(arguments)
