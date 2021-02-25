![img.png](img.png)


# rotten_tomatoes_scraping
A scraping program using BS4 to extracting the top 100 movies on netflix by RottenTomatoes website

Project overall process:

* Requesting responses from the website using grequest.
* Creating soup objects from responses using BS4, each soup represents a movie's page.
* Scraping useful data from the soup objects, by filtering HTML tags and classes.
* Arranging data in a pandas dataframe.
* Exporting the dataframe to an out.csv file

### Installation


Install the dependencies from the requirements text file, and run the main.py file
Using Python 3.8.5
```sh
$ git clone https://github.com/GalSalomon/rotten_tomatoes_scraping
$ cd rotten_tomatoes_scraping/
$ sudo pip3 install -r requirements.txt
```



### Todos
 - Filtering options
 - Add UI 
 - Write MORE Tests


### Credit

 Made by Gal Salomon and Oriel Pinhas during the "Israel Tech Challenge" program

