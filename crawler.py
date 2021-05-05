import numpy as np
import conf
import time
import grequests
import logging
import requests
import pandas as pd
import sys
import os
from bs4 import BeautifulSoup as soup
from datetime import datetime


formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")


def setup_logger(name, log_file, level=logging.INFO):
    """To setup few loggers"""
    handler = logging.FileHandler(log_file, 'w', 'utf-8')
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger


movies_logger = setup_logger('movies_log', 'movies.log')


def get_soup_from_response(response):
    """receives  a response from requests/grequests and returns soup"""
    page_html = response.content
    # html parser
    page_soup = soup(page_html, "html.parser")
    return page_soup


def get_soup_from_url(url):
    """function gets a url and returns it's parsed content in bs4.BeautifulSoup type"""
    try:
        movies_logger.info(f'Trying to get response for {url}')
        response = requests.get(url)
    except Exception as err:
        movies_logger.error(f'An error occurred: {err}')
        movies_logger.error(f'Exit program')
        sys.exit(1)
    movies_logger.info(f'Succeed getting response for {url}')
    return get_soup_from_response(response)


def get_responses_from_urls(urls):
    """receives urls and returns a dict:
    keys = urls
    values = responses"""
    try:
        # in order to prevent the site from blocking us: sleep 1 sec
        time.sleep(1)
        movies_logger.info(f'trying to get responses for next {len(urls)} urls')
        rs = (grequests.get(url) for url in urls)
        # If the response was successful, no Exception will be raised
        responses = grequests.map(rs)
        responses_dict = {}
        for i, url in enumerate(urls):
            responses_dict[url] = responses[i]
    except Exception as err:
        movies_logger.error(f'An error occurred: {err}')
        movies_logger.error(f'Exit program')
        sys.exit(1)
    movies_logger.info(f'Succeed getting responses for {len(urls)} urls')
    return responses_dict


def get_soups_from_urls(urls):
    """receives urls and returns a dict
    keys = urls
    values = soups"""
    responses_dict = get_responses_from_urls(urls)
    soups_dict = {}
    for key in responses_dict:
        movies_logger.info(f'Creating soup for {key}')
        soups_dict[key] = get_soup_from_response(responses_dict[key])
    return soups_dict


def get_titles_with_bs4(url):
    """function receives the url of the current top 100 movies on netflix
    and returns a list of 100 soups (including the name of the movie and url to it's
    page on rotten tomatoes)"""
    page_soup = get_soup_from_url(url)
    movies_logger.info(f'Fetching the current top 100 movies from Rotten Tomatoes {url}')
    titles = page_soup.findAll("div", {"class": "article_movie_title"})
    movies_logger.info(f'Finished fetching the current top 100 movies from Rotten Tomatoes {url}')
    return titles


def get_attributes_from_soup(key, movie_soup):
    """receives the soup of a movie url and returns a pandas df with the attributes"""
    movies_logger.info(f'Looking for movie attributes in soup of {key}')

    error = movie_soup.find("div", {"id": "mainColumn"}).get_text()
    if 'Internal Server Error' in error:
        movies_logger.error(f'{error} for {key}')
        return False
    if '404 - Not Found' in error:
        movies_logger.error(f'{error} for {key}')
        return False

    movies_logger.info(f'Looking for score-board in {key}')
    if movie_soup.findAll("score-board", {"audiencestate": "upright"}):
        move_profile = movie_soup.findAll("score-board", {"audiencestate": "upright"})
        movies_logger.info(f'score-board found for {key}!')
    elif movie_soup.findAll("score-board", {"audiencestate": "spilled"}):
        move_profile = movie_soup.findAll("score-board", {"audiencestate": "spilled"})
        movies_logger.info(f'score-board found for {key}!')
    else:
        move_profile = movie_soup.findAll("score-board", {"audiencestate": "N/A"})

    movies_logger.info(f'Looking for poster in {key}')
    movie_poster = movie_soup.find("img", {"class": "posterImage js-lazyLoad"})

    movies_logger.info(f'Looking for desc in {key}')
    movie_desc = movie_soup.find("div", {"id": "movieSynopsis"})
    if movie_desc:
        movie_desc = movie_desc.get_text().strip()
    if movie_poster:
        movie_poster = movie_poster.get('data-src')
    audience_score = move_profile[0].get('audiencescore')
    tomato_score = move_profile[0].get('tomatometerscore')
    h1_tag_text = move_profile[0].find('h1').get_text()
    p_tag_text = move_profile[0].find('p').get_text()

    list_of_p_values = p_tag_text.split(',')

    if len(list_of_p_values) == 3:
        year = list_of_p_values[0]
        genre = list_of_p_values[1]
        length = list_of_p_values[2]
    elif len(list_of_p_values) == 2:
        movies_logger.info(f"Didn't find length in {key}")
        year = list_of_p_values[0]
        genre = list_of_p_values[1]
        length = np.NaN
    elif len(list_of_p_values) == 1:
        movies_logger.info(f"Didn't find length in {key}")
        movies_logger.info(f"Didn't find genre in {key}")
        year = list_of_p_values[0]
        genre = np.NaN
        length = np.NaN
    else:
        movies_logger.info(f"Didn't find length in {key}")
        movies_logger.info(f"Didn't find genre in {key}")
        movies_logger.info(f"Didn't find year in {key}")
        year = np.NaN
        genre = np.NaN
        length = np.NaN

    movie_data = {
        "url": key,
        "title": h1_tag_text,
        "poster": movie_poster,
        "desc": movie_desc,
        "year": year,
        "genre": genre,
        "length": length,
        "audience_score": audience_score,
        "tomato_score": tomato_score,
    }
    movies_logger.info(f'Finished taking movie attributes for {key}')
    return movie_data


def add_data_to_movies_df(movies, movie_dict):
    """This function receives a movies with movie names and a dict of data of one movie.
    it fills the data of the dict into the df in the relevant line of the movie"""
    atts = movies.columns[1:]
    url = movie_dict['url']
    movies_logger.info(f"Adding the attributes of movie {movie_dict['title']} to the df")
    for att in atts:
        movies.at[url, att] = movie_dict[att]
    return movies


def get_rotten_tomatos_attributes_from_movies_urls(movies):
    """This function receives a df with movie title and a url for the movie's page on rotten tomatoes
     and returns a filled df, with additional attributes for each movie"""
    # We will work in batches in order to sent several requests in parallel
    for x in range(int(len(movies) / conf.BATCH_SIZE)):
        # runs from 0 till len(movies) / BATCH_SIZE
        movies_logger.info(f'Starting batch {x + 1}/{len(range(int(len(movies) / conf.BATCH_SIZE)))}')
        soups_dict = get_soups_from_urls(movies.index[x * conf.BATCH_SIZE:x * conf.BATCH_SIZE + conf.BATCH_SIZE])
        for key in soups_dict:
            movie_dict = get_attributes_from_soup(key, soups_dict[key])
            if movie_dict:
                movies = add_data_to_movies_df(movies, movie_dict)
            else:
                movies_logger.info(f'Could not get attributes from {key}')
                movies_logger.info(f'Moving on to the next url')
    return movies


def get_movie_titles_from_rotten_tomatos(url):
    """This function receives the url and of the top netflix movies on rotten tomatoes
    and returns a pandas df with the movie names and urls to their page in rotten tomatoes"""
    titles = get_titles_with_bs4(url)
    movies_logger.info(f'Creating a pandas df')
    movies = pd.DataFrame(
        columns=['title', 'url', 'genre', 'length', 'audience_score', 'tomato_score', 'year', 'poster', 'desc'])
    for t in titles:
        movie_name, href = t.a.contents[0], t.a['href']
        movies = movies.append(pd.DataFrame(columns=['title', 'url'], data=[[movie_name, href]]))
    movies_logger.info(f'Movie titles and url were added to the pandas df')
    movies = movies.set_index('url')
    return movies


def get_omdb_attributes_from_movies_urls(movies, url_omdbapi):
    """This function receives a pandas df with movies attributes from rotten tomatoes and
    adds two columns: imdb score and metacritic score"""
    movies_logger.info(f'Fetching scores from OMDb API')
    for x in range(int(len(movies) / conf.BATCH_SIZE)):
        # runs from 0 till len(movies) / BATCH_SIZE
        movies_logger.info(f'Starting batch {x + 1}/{len(range(int(len(movies) / conf.BATCH_SIZE)))}')
        idxs = movies.index[x * conf.BATCH_SIZE:x * conf.BATCH_SIZE + conf.BATCH_SIZE]
        titles = movies[movies.index.isin(idxs)]['title']
        urls = [f'http://www.omdbapi.com/?t={t}&apikey={os.environ.API_KEY}' for t in titles]
        responses_dict = get_responses_from_urls(urls)
        for key in responses_dict:

    return movies


def get_top_movies_data(url_rotten_tomatoes=conf.TOMATO_BEST_MOVIES, url_omdbapi=conf.OMDB_API_URL):
    """this function receives no input and:
    1. creates a pandas df with several attributes about the top netflix movies on rotten tomatoes
    2. saves it to csv
    """
    start = datetime.now()
    movies_logger.info(f'Starting to fetch all movies from {url_rotten_tomatoes} now!')
    # getting the names of the movies and their urls and putting them in a df
    print('Scraping Rotten Tomatoes! Please wait a moment')
    movies = get_movie_titles_from_rotten_tomatos(url_rotten_tomatoes)
    # fill the df with attributes of the movies
    movies = get_rotten_tomatos_attributes_from_movies_urls(movies)
    movies = get_omdb_attributes_from_movies_urls(movies, url_omdbapi)
    print('Done scraping!')
    print(f'This operation took {datetime.now() - start} ')
    movies_logger.info(f'Done!')
    movies_logger.info(f'This operation took {datetime.now() - start}')
    movies.to_csv('output.csv')
    return movies
