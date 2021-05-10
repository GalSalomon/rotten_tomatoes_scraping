import argparse
import time
import numpy as np

GENRES = ['Mystery and thriller', 'Music', 'Musical', 'Documentary', 'Drama',
          'Romance', 'Horror', 'War', 'Biography', 'Gay and lesbian', 'History',
          'Action', 'Crime', 'Comedy', 'Sci fi', 'Fantasy', 'Adventure', 'Kids and family', 'Animation',
          'Sports and fitness', 'Other']

MIN_YEAR_ALLOWED = 1960
MAX_YEAR_ALLOWED = time.localtime().tm_year
YEARS_ALLOWED = np.arange(MIN_YEAR_ALLOWED, MAX_YEAR_ALLOWED + 1)
SCORES_ALLOWED = ['Tomato', 'IMDb', 'Both']
MIN_LIMIT_ALLOWED = 5
MAX_LIMIT_ALOWED = 100
LIMIT_ALLOWED = np.arange(MIN_LIMIT_ALLOWED, MAX_LIMIT_ALOWED)


def create_parser(arguments):
    """Returns a dict of argparse.ArgumentParser"""
    parser = argparse.ArgumentParser(description='Get the current top Netflx movies accorting to Rotten Tomatoes and IMDb.')
    parser.add_argument('-s', '--score', metavar='', choices=SCORES_ALLOWED,
                        default='Both', help='The score used. Could be Tomato or IMDb. Default is Both')
    parser.add_argument('-l', '--limit', metavar='', choices=LIMIT_ALLOWED,
                        type=int, default=10, help='Limit of results. Default 10')
    parser.add_argument('-min_y', '--min_year', metavar='', choices=YEARS_ALLOWED,
                        type=int, help='Minimum year. Default None')
    parser.add_argument('-max_y', '--max_year', metavar='', choices=YEARS_ALLOWED,
                        type=int, help='Miximum year. Default None')
    parser.add_argument('-g', '--geners', metavar='', type=str, nargs='*',
                        choices=GENRES, help='List of Geners to filter. Default None')
    args = parser.parse_args()
    args_dict = vars(args)
    return args_dict
