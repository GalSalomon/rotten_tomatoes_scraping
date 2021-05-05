import requests
import os
import ast


def get_metacritic_and_imdbscore(movie_title):
    """This function gets a movie title and returns it's imdb and metacritic scores"""
    url = f'http://www.omdbapi.com/?t={movie_title}&apikey={os.environ.API_KEY}'
    try:
        response = requests.get(url)
    except Exception as err:
        print(f'error: {err}')

    content_dict = ast.literal_eval(response.content.decode("UTF-8"))
    imdb_score = content_dict['imdbRating']
    metacritic_score = content_dict['Metascore']
    return imdb_score, metacritic_score





def get_movie_scores(movie_list):
    """
    :param movie_list: gets a list of movies
    :return: dataframe of movie titles, imdb scores, metacritic scores
    """
    pass


def add_scores_to_database(scores):
    """
    :param scores: gets a dataframe of movie titles, imdb scores and metacritic scores
    And adds the scores to the database.
    """
    pass
