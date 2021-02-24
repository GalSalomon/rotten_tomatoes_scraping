from pprint import pprint

import numpy as np

import conf
import grequests
import logging
import requests
import pandas as pd
import sys
from bs4 import BeautifulSoup as soup
from datetime import datetime

formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')


def setup_logger(name, log_file, level=logging.INFO):
    """To setup few loggers"""
    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger


movies_logger = setup_logger('movies_log', 'movies.log')


def get_soup_from_response(response):
    # this function is DONE!
    """receives  a response from requests/grequests and returns soup"""
    page_html = response.content
    # html parser
    movies_logger.debug(f'Parsing the response to soup')
    page_soup = soup(page_html, "html.parser")
    return page_soup


def get_soup_from_url(url):
    """function gets a url and returns it's parsed content in bs4.BeautifulSoup type"""
    try:
        movies_logger.info(f'Trying to get response for {url}')
        response = requests.get(url)
    except Exception as err:
        movies_logger.error(f'An error occurred: {err}')  # todo: add more exceptions and explain in details
        movies_logger.error(f'Exit program')
        sys.exit(1)
    movies_logger.info(f'Succeed getting response for {url}')
    return get_soup_from_response(response)


def get_responses_from_urls(urls):
    # this function is DONE!
    """receives urls and returns responses"""
    try:
        movies_logger.info(f'trying to get responses for next {len(urls)} urls')
        rs = (grequests.get(url) for url in urls)
        # If the response was successful, no Exception will be raised
        responses = grequests.map(rs)
    except Exception as err:
        movies_logger.error(f'An error occurred: {err}')
        movies_logger.error(f'Exit program')
        sys.exit(1)
    movies_logger.info(f'Succeed getting responses for {len(urls)} urls')
    return responses


def get_soups_from_urls(urls):
    """receives urls and returns a list of soups"""
    responses = get_responses_from_urls(urls)
    return [get_soup_from_response(response) for response in responses]


def get_titles_with_bs4(url):  
    """function receives the url of the current top 100 movies on netflix
    and returns a list of 100 soups (including the name of the movie and url to it's
    page on rotten tomatoes)"""
    page_soup = get_soup_from_url(url)
    movies_logger.info(f'Fetching the current top 100 movies from Rotten Tomatoes {url}')
    titles = page_soup.findAll("div", {"class": "article_movie_title"})
    movies_logger.info(f'Finished fetching the current top 100 movies from Rotten Tomatoes {url}')
    return titles


def get_attributes_from_soup(movie_soup):
    # todo fix this function to get all the required data from the movie page
    """receives the soup of a movie url and returns a pandas df with the attributes"""
    movies_logger.info(f'Looking for movie attributes in soup')
    error = movie_soup.find("div", {"id": "mainColumn"}).get_text()

    if 'Internal Server Error' in error:
        print(error)
        return False

    if '404 - Not Found' in error:
        print(error)
        return False

    # attrs = ['title', 'url', 'genre', 'length', 'score1', 'score2', 'year', 'poster', 'text']

    if movie_soup.findAll("score-board", {"audiencestate": "upright"}):
        move_profile = movie_soup.findAll("score-board", {"audiencestate": "upright"})
    elif movie_soup.findAll("score-board", {"audiencestate": "spilled"}):
        move_profile = movie_soup.findAll("score-board", {"audiencestate": "spilled"})
    else:
        move_profile = movie_soup.findAll("score-board", {"audiencestate": "N/A"})

    move_poster = movie_soup.find("img", {"class": "posterImage js-lazyLoad"})
    move_desc = movie_soup.find("div", {"id": "movieSynopsis"})
    if move_desc:
        move_desc = move_desc.get_text().strip()

    if move_poster:
        move_poster = move_poster.get('data-src')

    audience_score = move_profile[0].get('audiencescore')
    tomato_score = move_profile[0].get('tomatometerscore')
    h1_tag_text = move_profile[0].find('h1').get_text()
    p_tag_text = move_profile[0].find('p').get_text()

    list_of_p_values = p_tag_text.split(',')
    # year, genre, length
    print(movie_soup.findAll({"rel": "cannonical"}))

    if len(list_of_p_values) == 3:
        year = list_of_p_values[0]
        genre = list_of_p_values[1]
        length = list_of_p_values[2]
    elif len(list_of_p_values) == 2:
        year = list_of_p_values[0]
        genre = list_of_p_values[1]
        length = np.NaN
    elif len(list_of_p_values) == 1:
        year = list_of_p_values[0]
        genre = np.NaN
        length = np.NaN
    else:
        year = np.NaN
        genre = np.NaN
        length = np.NaN

    movie_data = {
        "title": h1_tag_text,
        "poster": move_poster,
        "desc": move_desc,
        "year": year,
        "genre": genre,
        "length": length,
        "audience score": audience_score,
        "tomato score": tomato_score,
    }
    return movie_data


def get_attributes_from_movies_urls(movies):
    """gets a data frame with movie title and a link for the movie's page on rotten tomatoes
     and returns a filled data frame, with the following additional columns.
    """

    for x in range(int(len(movies) / conf.BATCH_SIZE)):
        # runs from 0 till len(movies) / BATCH_SIZE
        movies_logger.info(f'Starting batch {x + 1}/{len(range(int(len(movies) / conf.BATCH_SIZE)))}')
        soups = get_soups_from_urls(movies['url'][x * conf.BATCH_SIZE:x * conf.BATCH_SIZE + conf.BATCH_SIZE])
        soups_index = 0
        for movie_soup in range(x * conf.BATCH_SIZE, x * conf.BATCH_SIZE + conf.BATCH_SIZE):
            dictionary = get_attributes_from_soup(soups[soups_index])
            pprint(dictionary)
            if dictionary:
                movies = movies.append(dictionary, ignore_index=True)
            soups_index += 1
    return movies


def get_titles_movies(url, movies):
    titles = get_titles_with_bs4(url)
    for t in titles:
        movie_name, href = t.a.contents[0], t.a['href']
        movies = movies.append(pd.DataFrame(columns=['title', 'url'], data=[[movie_name, href]]))
    return movies


def get_top_movies_on_rotten_tomatoes(url=conf.TOMATO_BEST_MOVIES):
    """this function gets no input and creates a pandas df with several attributes
    about the top netflix movies on rotten tomatoes"""
    start = datetime.now()
    movies_logger.info(f'Starting to fetch all movies from {url} now!')
    movies = pd.DataFrame(columns=['title', 'url', 'genre', 'length', 'score1', 'score2', 'year', 'poster', 'text'])
    movies = get_titles_movies(url, movies)

    # movies = movies.set_index('title')
    movies = get_attributes_from_movies_urls(movies)
    print(f'this operation took {datetime.now() - start} ')
    movies_logger.info(f'Done!')
    movies_logger.info(f'This operation took {datetime.now() - start}')
    print(movies)
    return movies


if __name__ == "__main__":
    df1 = get_top_movies_on_rotten_tomatoes()
    df1.to_csv('out.csv')
