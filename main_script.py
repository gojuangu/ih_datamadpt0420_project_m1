import argparse
from i_gathering import d_gathering as dga
from i_cleaning import d_cleaning as dcl
#from p_analysis import m_analysis as man
#from p_reporting import m_reporting as mre

def argument_parser():
    parser = argparse.ArgumentParser(description='specify inputs')
    parser.add_argument('-u', "--url", type=str, help='specify path web scraping', required=True)
    parser.add_argument('-p', "--path", type=str, help='specify database', required=True)
    args = parser.parse_args()

    return args


def main(arguments):
    print('starting pipeline...')
    bd = dga.get_info(arguments.path)
    countries = dga.get_country(arguments.url)
    #jobs = dga.get_info()

    print('pipeline finished...')


if __name__ == '__main__':
    arguments = argument_parser()
    main(arguments)
