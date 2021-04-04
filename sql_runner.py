import pandas as pd
from sql_wizard import connect_to_mysql, end_mysql_db_connection_without_cursor, login_credentials

GENRES = ['Mystery and thriller', 'Music', 'Musical', 'Documentary', 'Drama',
          'Romance', 'Horror', 'War', 'Biography', 'Gay and lesbian', 'History',
          'Action', 'Crime', 'Comedy', 'Sci fi', 'Fantasy', 'Adventure', 'Kids and family', 'Animation',
          'Sports and fitness', 'Other']

GENRES_DICT = {'Mystery and thriller': 0, 'Music': 1, 'Musical': 2, 'Documentary': 3, 'Drama': 4,
               'Romance': 5, 'Horror': 6, 'War': 7, 'Biography': 8, 'Gay and lesbian': 9, 'History': 10,
               'Action': 11, 'Crime': 12, 'Comedy': 13, 'Sci fi': 14, 'Fantasy': 15, 'Adventure': 16,
               'Kids and family': 17, 'Animation': 18, 'Sports and fitness': 19, 'Other': 20}


def query_options(dict_run):
    """
    gets a dicts, returns a query by this dict options
    """
    if dict_run['score'] == 'Tomato':
        sql_command = ['SELECT url,title, length, year, poster,tomato_score FROM movies ']
    if dict_run['score'] == 'Audience':
        sql_command = ['SELECT url,title, length, year, poster,audience_score FROM movies ']
    if dict_run['score'] == 'Both':
        sql_command = ['SELECT url,title, length, year, poster, ((movies.audience_score)+(movies.tomato_score))/2 as '
                       'Average FROM movies ']

    if dict_run['geners'] is not None:
        genre_to_id_list = [GENRES_DICT[x] for x in dict_run['geners']]
        if len(genre_to_id_list) > 1:
            t = tuple(genre_to_id_list)
        else:
            t = '(' + str(genre_to_id_list[0]) + ')'
        sql_command.append('INNER JOIN movie_genre ON movie_genre.movie_id = movies.index  INNER JOIN genres ON '
                           'movie_genre.genre_id = genres.index AND movie_genre.genre_id in {}'.format(t))

    if dict_run['min_year'] is None and dict_run['max_year'] is not None:
        sql_command.append(' WHERE year < ' + str(dict_run['max_year']))
    elif dict_run['min_year'] is not None and dict_run['max_year'] is None:
        sql_command.append(' WHERE year > ' + str(dict_run['min_year']))
    elif dict_run['min_year'] is not None and dict_run['max_year'] is not None:
        sql_command.append(' WHERE year BETWEEN ' + str(dict_run['min_year']) + ' AND ' + str(dict_run['max_year']))

    if dict_run['score'] == 'Tomato':
        sql_command.append(' ORDER BY tomato_score DESC')
    if dict_run['score'] == 'Audience':
        sql_command.append(' ORDER BY audience_score DESC')
    if dict_run['score'] == 'Both':
        sql_command.append(' ORDER BY Average DESC')

    if dict_run['limit'] is not None:
        sql_command.append(' LIMIT ' + str(dict_run['limit']))

    return ''.join(sql_command)


def running_query(dict_run, db_name='no_db'):
    """
    gets credentials, a dict and a database name
    runs a query on the database
    """
    sql = query_options(dict_run)
    print('connecting to mysql with database name')
    db_connection = connect_to_mysql(db_name)
    sql_data = pd.read_sql(sql, con=db_connection)
    print(sql_data)
    end_mysql_db_connection_without_cursor(db_connection)


def run(dict_run):
    db_name = 'movies'
    login_credentials(db_name)
    running_query(dict_run, db_name)
