import mysql.connector
from mysql.connector import Error
import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine

GENRES = ['Mystery and thriller', 'Music', 'Musical', 'Documentary', 'Drama',
          'Romance', 'Horror', 'War', 'Biography', 'Gay and lesbian', 'History',
          'Action', 'Crime', 'Comedy', 'Sci fi', 'Fantasy', 'Adventure', 'Kids and family', 'Animation',
          'Sports and fitness', 'Other']

GENRES_DICT = {'Mystery and thriller': 0, 'Music': 1, 'Musical': 2, 'Documentary': 3, 'Drama': 4,
               'Romance': 5, 'Horror': 6, 'War': 7, 'Biography': 8, 'Gay and lesbian': 9, 'History': 10,
               'Action': 11, 'Crime': 12, 'Comedy': 13, 'Sci fi': 14, 'Fantasy': 15, 'Adventure': 16,
               'Kids and family': 17, 'Animation': 18, 'Sports and fitness': 19, 'Other': 20}


def login_credentials(db_name='no_db'):
    """
    creates a .env file with login credentials:
    first entry is the host name
    2nd entry is the username
    3rd entry is the password
    4th entry is the database name, if there isn't any
    """

    PATH = '.env'
    if os.path.isfile(PATH) and os.access(PATH, os.R_OK):
        with open(".env", "r") as f:
            for line in f.readlines():
                try:
                    key, value = line[:-1].split('=')
                    os.environ[key] = value

                except ValueError:
                    # syntax error
                    pass

    else:
        credentials = [input("Enter host name: "), input("Enter username: "), input("Enter password: "), db_name]
        os.environ['hostname'] = credentials[0]
        os.environ['username'] = credentials[1]
        os.environ['password'] = credentials[2]

        with open(".env", "w") as f:
            f.write('hostname=')
            f.write(os.environ['hostname'])
            f.write("\n")
            f.write('username=')
            f.write(os.environ['username'])
            f.write("\n")
            f.write('password=')
            f.write(os.environ['password'])
            f.write("\n")


def connect_to_mysql(db_name='no_db'):
    """
    Establish a mysql connection to a database,
    if there isn't any database, it will connect to the database
    without the database parameter
    :return: A db_connection object
    """
    # Login without db name
    if db_name == 'no_db':
        try:
            _db_connection = mysql.connector.connect(
                host=os.environ['hostname'],
                user=os.environ['username'],
                passwd=os.environ['password']
            )
        except Error as e:
            print(e)
        print('connected to mysql!')

    # Login with db name
    else:
        try:
            _db_connection = mysql.connector.connect(
                host=os.environ['hostname'],
                user=os.environ['username'],
                passwd=os.environ['password'],
                database=db_name
            )
        except Error as e:
            print(e)
        print('connected to mysql database!')

    return _db_connection


def end_mysql_db_connection_without_cursor(db_connection):
    """
    :param db_connection: an active database connection
    :param mycursor: a cursor
    Closing the connection
    """
    if db_connection.is_connected():
        db_connection.close()
        print("MySQL connection is closed")


def end_mysql_db_connection(db_connection, mycursor):
    """
    :param db_connection: an active database connection
    :param mycursor: a cursor
    Closing the connection
    """
    if db_connection.is_connected():
        mycursor.close()
        db_connection.close()
        print("MySQL connection is closed")


def create_movie_database(db_name='no_db'):
    """
    getting a db_name,
    Connecting and creating database named db_name
    """
    print('connecting to mysql without database name')
    db_connection = connect_to_mysql()
    mycursor = db_connection.cursor()
    if db_name == 'no_db':
        db_name = input('Enter new database name: ')

    mycursor.execute("DROP DATABASE if exists " + str(db_name))
    mycursor.execute("CREATE DATABASE " + db_name)
    print(db_name, 'database created!')
    end_mysql_db_connection(db_connection, mycursor)


def create_engine_sqlalchemy(db_name):
    """
    getting a db_name,
    Creating a sqlalchemy engine
    """
    return create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
                         .format(host=os.environ['hostname'], db=db_name, user=os.environ['username'],
                                 pw=os.environ['password']))




def movie_genres_to_dict(df):
    """
    getting dataframe,
    returning a dict of genres
    """
    dict_genre = {}
    for i in range(len(df)):
        if type(df.loc[i, "genre"]) == str:
            list_genre = df.loc[i, "genre"].split('/')
            list_genre_to_id = []
            for genre in list_genre:
                list_genre_to_id.append(GENRES_DICT[genre.strip()])
            dict_genre[i] = list_genre_to_id
    return dict_genre


def create_table_movie_genre(df, db_name):
    """
    getting dataframe, db_name
    Creating movie_genre table
    Entering movie_id - the movie id, genre_id - the genre id
    """
    db_connection = connect_to_mysql(db_name)
    mycursor = db_connection.cursor()

    mycursor.execute("CREATE TABLE movie_genre (movie_id int, genre_id int)")
    print('movie_genre table created!')

    sql = "INSERT INTO movie_genre (movie_id, genre_id) VALUES (%s, %s)"
    for i in range(len(df)):
        for element in list(df.iloc[i]):
            if not np.isnan(element):
                val = (i, element)
                mycursor.execute(sql, val)
    db_connection.commit()
    end_mysql_db_connection(db_connection, mycursor)


def create_table_disc(df, db_name):
    """
    getting dataframe, db_name
    Creating description table
    Entering movie_id - the movie id, movie_desc - the description
    """
    db_connection = connect_to_mysql(db_name)
    mycursor = db_connection.cursor()

    mycursor.execute("CREATE TABLE description (movie_id int, movie_desc varchar(8000))")
    print('description table created!')

    sql = "INSERT INTO description (movie_id, movie_desc) VALUES (%s, %s)"
    for i in range(len(df)):
        for element in list(df.iloc[i]):
            if type(element) == str:
                val = (i, element)
                mycursor.execute(sql, val)
    db_connection.commit()
    end_mysql_db_connection(db_connection, mycursor)


def create_movie_tables(engine, df, db_name):
    """
    Getting engine, dataframe, db_name
    Creating the tables: movies, genres and movie_genre
    """
    movie = df.drop(['genre', 'desc'], axis=1)
    genre = pd.DataFrame(data=GENRES, columns=['genre'])

    genre.to_sql('genres', engine)
    movie.to_sql('movies', engine)

    genre_to_movie_id = pd.DataFrame.from_dict(movie_genres_to_dict(df[['genre']]), orient='index')
    create_table_movie_genre(genre_to_movie_id, db_name)
    create_table_disc(df[['desc']], db_name)


def create_tables(db_name='no_db'):
    """
    gets a database name
    creates all tables of movie database
    """
    df = pd.read_csv('output.csv')

    if db_name == 'no_db':
        db_name = input('Enter database name: ')

    engine = create_engine_sqlalchemy(db_name)

    create_movie_tables(engine, df, db_name)


def run():
    print('creating database')
    db_name = 'movies'
    login_credentials(db_name)
    create_movie_database(db_name)
    create_tables(db_name)
    print('done!')
