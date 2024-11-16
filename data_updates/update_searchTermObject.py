import requests
import json

search_categories_url = "http://localhost:5000/searchCategories/"
search_term_object_url = "http://localhost:5000/searchData/"

try:
    response = requests.get(search_categories_url)
    categories_arr = response.json()

    try:
        #post request to get the search term object, with the category as the body
        categories_dict = {"categories": categories_arr}
        response = requests.post(search_term_object_url, json=categories_dict)
        data = response.json()

        with open("midas-viz-data/searchTermObject.json", "w") as outfile:
             json.dump(data, outfile)

    except requests.exceptions.RequestException as e:
        print(e)
        exit()
except requests.exceptions.RequestException as e:
    print(e)
    exit()
