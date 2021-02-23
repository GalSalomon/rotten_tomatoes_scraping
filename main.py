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
    """To setup a logger"""
    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger


movies_logger = setup_logger('movies_log', 'movies.log')


def get_soup_from_response(response):
    """this function gets a response from requests/grequests and returns soup"""
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
    """receives list of urls and returns responses"""
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


def get_soups_from_urls(urls_batch):
    """receives dict of key movie titles and values urls
    and returns a dict of keys movie titles and values soups"""
    keys = list(urls_batch.keys())
    responses = get_responses_from_urls(list(urls_batch.values()))
    responses_dict = {}
    for i, key in enumerate(keys):
        responses_dict[key] = get_soup_from_response(responses[i])
    return responses_dict


def get_titles_with_bs4(url):
    """function receives the url of the current top 100 movies on netflix
    and returns a list of 100 soups (including the name of the movie and url to it's
    page on rotten tomatoes)"""
    page_soup = get_soup_from_url(url)
    movies_logger.info(f'Fetching the current top 100 movies from Rotten Tomatoes {url}')
    titles = page_soup.findAll("div", {"class": "article_movie_title"})
    movies_logger.info(f'Finished fetching the current top 100 movies from Rotten Tomatoes {url}')
    return titles


def add_attributes_from_soup(movie_soup):
    """This function receives the soup of a movie url.
    If the soup is valid, returns a pandas df of one row with the attributes of the movie.
    If not, it returns False"""
    movies_logger.info(f'Looking for movie attributes in soup')
    if movie_soup.find("div", {"id": "mainColumn"}).get_text():
        return False
    # todo remove the next line
    # attrs = ['title', 'url', 'genre', 'length', 'score1', 'score2', 'year', 'poster', 'text']
    # the scores, year, genre and length are in the profile
    movie_poster_html = movie_soup.find("img", {"class": "posterImage js-lazyLoad"})
    movie_desc_html = movie_soup.find("div", {"id": "movieSynopsis"})
    # movie profile could be in upright or spilled classes:
    if movie_soup.findAll("score-board", {"audiencestate": "upright"}):
        movie_profile_html = movie_soup.findAll("score-board", {"audiencestate": "upright"})
    else:
        movie_profile_html = movie_soup.findAll("score-board", {"audiencestate": "spilled"})

    # movie_desc_html might be missing
    if movie_desc_html:
        movie_desc = movie_desc_html.get_text().strip()
    else:
        print("we got to do something here")  # todo
    if movie_poster_html:
        movie_poster = movie_poster_html.get('data-src')
    else:
        print("we got to do something here")  # todo

    # print(title)
    # score
    # poster link      ("div", {"class": "credit_summary_item"}):
    # text desc data-qa="critics-consensus"
    audience_score = movie_profile_html[0].get('audiencescore')
    tomato_score = movie_profile_html[0].get('tomatometerscore')
    h1_tag_text = movie_profile_html[0].find('h1').get_text()
    p_tag_text = movie_profile_html[0].find('p').get_text()
    year, genre, length = p_tag_text.split(',')
    # columns = ['title', 'url', 'genre', 'length', 'score1', 'score2', 'year', 'poster', 'desc']
    print("poster", movie_poster)
    print("title: ", h1_tag_text)
    print("desc: ", movie_desc)
    print("year: ", year)
    print("genre: ", genre)
    print("length: ", length)
    print("audience score: ", audience_score)
    print("tomato score: ", tomato_score)
    print("/////////NEXT/MOVIE////////")
    return "/////NEXT/BATCH////////"


def get_attributes_from_movies_urls(movies_urls):
    """this function receives a dict with movies titles and links for the movies
    pages on rotten tomatoes and returns a dataframe, with the following additional columns."""
    keys = list(movies_urls.keys())
    for x in range(int(len(keys) / conf.BATCH_SIZE)):
        # runs from 0 till len(keys) / BATCH_SIZE
        movies_logger.info(f'Starting batch {x + 1}/{len(range(int(len(keys) / conf.BATCH_SIZE)))}')
        urls_batch = {}
        for key in keys[x * conf.BATCH_SIZE: x * conf.BATCH_SIZE + conf.BATCH_SIZE]:
            urls_batch[key] = movies_urls[key]
        soups = get_soups_from_urls(urls_batch)
        # soups_index = 0
        for i in range(x * conf.BATCH_SIZE, x * conf.BATCH_SIZE + conf.BATCH_SIZE):
            atts = add_attributes_from_soup(soups[soups_index])
            print(atts)
            if atts:
                pass  # todo update movies df with movie
            else:
                pass
            soups_index += 1
    return movies


def get_titles(url):
    """function receives the url of the top netflix movies on rotten tomatoes
    and returns a dict ot movie titles and urls """
    movies_logger.info(f'Starting to fetch all movie titles from {url} now!')
    titles = get_titles_with_bs4(url)
    # creates the dict of titles and urls
    movies_urls = {}
    for title in titles:
        movie_name, href = title.a.contents[0], title.a['href']
        movies_urls[movie_name] = href
    return movies_urls


def get_top_movies_on_rotten_tomatoes(url=conf.TOMATO_BEST_MOVIES):
    """this function receives no input and creates a pandas df with several attributes
    of the top netflix movies on rotten tomatoes"""
    start = datetime.now()
    # get movie titles
    movies_urls = get_titles(url)
    # get the attributes of the movies
    movies_df = get_attributes_from_movies_urls(movies_urls)
    movies_logger.info(f'Done!')
    movies_logger.info(f'This operation took {datetime.now() - start}')
    return movies_df


if __name__ == "__main__":
    df = get_top_movies_on_rotten_tomatoes()
