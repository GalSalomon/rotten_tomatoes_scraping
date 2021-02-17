import conf
import grequests
import logging
import requests
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
    """receives  a response from requests/grequests and returns soup"""
    page_html = response.content
    # html parser
    movies_logger.debug(f'Parsing the response to soup')
    page_soup = soup(page_html, "html.parser")
    return page_soup


def get_responses_from_urls(urls):
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


def get_soup_from_url(url):
    """gets a url and returns it's parsed content in bs4.BeautifulSoup type """
    try:
        movies_logger.info(f'Trying to get response for {url}')
        response = requests.get(url)
    except Exception as err:
        movies_logger.error(f'An error occurred: {err}')
        movies_logger.error(f'Exit program')
        sys.exit(1)
    movies_logger.info(f'Succeed getting response for {url}')
    return get_soup_from_response(response)


def get_directors_from_soup(movie_soup):
    """gets soup of a movie url and returns the director/s from the page"""
    movies_logger.info(f'Looking for directors in the soup')
    credit_items = movie_soup.findAll("div", {"class": "credit_summary_item"})
    # Find the director tag
    directors_tag = [c for c in credit_items if "Director" in c.h4.contents[0]][0]
    # get the director/s names
    directors_lst = [x.contents[0] for x in directors_tag.find_all('a')]
    if len(directors_lst) == 0:
        movies_logger.error(f"Didn't find directors for this movie...")
    elif len(directors_lst) == 1:
        movies_logger.info(f"One director was found and stored")
    else:
        movies_logger.info(f"Found more than one director. All were stored")
    return ", ".join(directors_lst)


def get_titles_with_bs4(url):
    page_soup = get_soup_from_url(url)
    movies_logger.info(f'Fetching the movie titles of iMDB top 250 from {url}')
    titles = page_soup.findAll("td", {"class": "titleColumn"})
    movies_logger.info(f'Finished fetching the movie titles of iMDB top 250 from {url}')
    return titles


def add_directors_with_grequests(movies):
    """gets a list of dictionaries, each dict is a movie
    and added a new key "directors" with a value of the movie's directors"""
    for x in range(int(len(movies) / conf.BATCH_SIZE)):
        urls = []
        movies_logger.info(f'Starting batch {x + 1}/{len(range(int(len(movies) / conf.BATCH_SIZE)))}')
        for i in range(x * conf.BATCH_SIZE, x * conf.BATCH_SIZE + conf.BATCH_SIZE):
            urls.append(movies[i]['url'])
        soups = get_soups_from_urls(urls)
        soups_index = 0
        for i in range(x * conf.BATCH_SIZE, x * conf.BATCH_SIZE + conf.BATCH_SIZE):
            movies[i]['directors'] = get_directors_from_soup(soups[soups_index])
            soups_index += 1
            print(f"{i + 1} - {movies[i]['movie_name']} - {movies[i]['directors']}")
            movies_logger.info(f"{i + 1} - {movies[i]['movie_name']} - {movies[i]['directors']}")
    return movies


def get_imdb_top_250_with_grequests(url=conf.IMDB_TOP_250_URL):
    """this function gets no input and prints the top 250 movies and their directors according to imdb
    but this time with grequests"""
    start = datetime.now()
    movies_logger.info('Starting now! ("grequests" fast version)')
    titles = get_titles_with_bs4(url)
    movies = []
    for t in titles:
        movie_name, url = t.a.contents[0], "https://www.imdb.com" + t.a['href']
        movies.append({'movie_name': movie_name, 'url': url})
    movies = add_directors_with_grequests(movies)
    print(f'this operation took {datetime.now() - start}')
    movies_logger.info(f'Done!')
    movies_logger.info(f'This operation took {datetime.now() - start}')
    return movies


if __name__ == "__main__":
    get_imdb_top_250_with_grequests()

"""
# Todo:
ask if we can use grequests
ask if we can use pandas:

we plan to fill this table:

movies:
title, genre, length, score1, score2, year, poster, text
"""
