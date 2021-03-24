import unittest
import main
import pandas as pd
from bs4 import BeautifulSoup as soup


class TestStringMethods(unittest.TestCase):

    def test_get_titles_movies(self):
        """
        Test to see if the function get_titles_movies returns a data frame object
        """
        data = main.get_titles_movies(
            'https://editorial.rottentomatoes.com/guide/best-netflix-movies-to-watch-right-now/')
        df = pd.DataFrame()
        self.assertEqual(type(df), type(data))

    def test_get_attributes_from_soup(self):
        """
        Test that we are getting attributes from local files with the soup object
        and that the data is accurate
        """
        with open("tests/departed.html", "rb") as f:
            soup_object1 = soup(f.read(), features='html.parser')

        with open("tests/1922_2017.html", "rb") as f:
            soup_object2 = soup(f.read(), features='html.parser')

        function_dict_soup_1 = main.get_attributes_from_soup('https://www.rottentomatoes.com/m/departed', soup_object1)
        test_dict_for_soup_1 = {'url': 'https://www.rottentomatoes.com/m/departed', 'title': 'The Departed',
                                'poster': 'https://resizing.flixster.com/cpZ3WiuL4SQODLSNN-Zjwve_HHs=/206x305/v2/https://flxt.tmsimg.com/NowShowing/54979/54979_aa.jpg',
                                'desc': "South Boston cop Billy Costigan (Leonardo DiCaprio) goes under cover to infiltrate the organization of gangland chief Frank Costello (Jack Nicholson). As Billy gains the mobster's trust, a career criminal named Colin Sullivan (Matt Damon) infiltrates the police department and reports on its activities to his syndicate bosses. When both organizations learn they have a mole in their midst, Billy and Colin must figure out each other's identities to save their own lives.",
                                'year': '2006', 'genre': ' Crime/Mystery and thriller', 'length': ' 2h 32m',
                                'audience_score': '94', 'tomato_score': '90'}
        assert function_dict_soup_1['url'] == test_dict_for_soup_1['url']
        assert function_dict_soup_1['title'] == test_dict_for_soup_1['title']
        assert function_dict_soup_1['poster'] == test_dict_for_soup_1['poster']
        assert function_dict_soup_1['desc'] == test_dict_for_soup_1['desc']
        assert function_dict_soup_1['genre'] == test_dict_for_soup_1['genre']
        assert function_dict_soup_1['audience_score'] == test_dict_for_soup_1['audience_score']
        assert function_dict_soup_1['tomato_score'] == test_dict_for_soup_1['tomato_score']
        assert function_dict_soup_1['length'] == test_dict_for_soup_1['length']

        function_dict_soup_2 = main.get_attributes_from_soup('https://www.rottentomatoes.com/m/1922_2017',
                                                             soup_object2)
        test_dict_for_soup_2 = {'url': 'https://www.rottentomatoes.com/m/1922_2017', 'title': '1922',
                                'poster': 'https://resizing.flixster.com/kBMRd_diQ5VYuIGLgvqXqiDabfI=/206x305/v2/https://resizing.flixster.com/NUpl7rLev_X0tjQf1vDOPhRUeuI=/ems.ZW1zLXByZC1hc3NldHMvbW92aWVzL2I5YTk5NWJhLTc5NTgtNGM0MS05ZjJhLTBmZjM2NmQ4ZDFhZi53ZWJw',
                                'desc': 'A rancher conspires to murder his wife for financial gain and convinces his teenage son to participate.',
                                'year': '2017', 'genre': ' Horror', 'length': ' 1h 42m', 'audience_score': '57',
                                'tomato_score': '91'}
        assert function_dict_soup_2['url'] == test_dict_for_soup_2['url']
        assert function_dict_soup_2['title'] == test_dict_for_soup_2['title']
        assert function_dict_soup_2['poster'] == test_dict_for_soup_2['poster']
        assert function_dict_soup_2['desc'] == test_dict_for_soup_2['desc']
        assert function_dict_soup_2['genre'] == test_dict_for_soup_2['genre']
        assert function_dict_soup_2['audience_score'] == test_dict_for_soup_2['audience_score']
        assert function_dict_soup_2['tomato_score'] == test_dict_for_soup_2['tomato_score']
        assert function_dict_soup_2['length'] == test_dict_for_soup_2['length']

    def test_titles_wit_bs4_returns_soup(self):
        """
        Test to see if the function get_titles_with_bs4 returns a soup object
        """
        data = main.get_titles_with_bs4(
            'https://editorial.rottentomatoes.com/guide/best-netflix-movies-to-watch-right-now/')
        soup_object = soup()
        assert (isinstance(type(data), type(soup_object)))


if __name__ == '__main__':
    unittest.main()
