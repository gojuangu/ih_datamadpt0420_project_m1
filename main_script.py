import argparse
from i_gathering import d_gathering as dga



def argument_parser():
    parser = argparse.ArgumentParser(description='specify inputs')
    parser.add_argument('-u', "--url", type=str, help='specify path web scraping', required=True)
    parser.add_argument('-p', "--path", type=str, help='specify database', required=True)
    args = parser.parse_args()

    return args


def main(arguments):
    print('starting pipeline...')
    df_all_tables = dga.get_info(arguments.path)
    countries_df = dga.get_country(arguments.url)
    jobs_ids = dga.job_ids(df_all_tables)
    jobs_df = dga.get_job(jobs_ids)
    merge = dga.all_merged_to_csv(countries_df, jobs_df, df_all_tables)

    print('pipeline finished...')


if __name__ == '__main__':
    arguments = argument_parser()
    main(arguments)
