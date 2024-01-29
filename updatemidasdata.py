import json
import os.path

import requests

BASE_URL = "https://members.midasnetwork.us/midascontacts/query/"
VISUALIZER_SUFFIX = "/visualizer/all"

ENDPOINTS = {
    "PEOPLE_ENDPOINT": "people",
    "PAPERS_ENDPOINT": "papers",
    "GRANTS_ENDPOINT": "projects",
    "ORGANIZATIONS_ENDPOINT": "organizations"
}
API_KEY = ""
OUTPUT_DIR = "midas-data"

##make function to read APIKEY from a file

def fetch_midas_data(endpoint):
    return requests.get(BASE_URL + endpoint + VISUALIZER_SUFFIX + "?apiKey=" + API_KEY)


def main():
    for endpoint in ENDPOINTS:
        data = fetch_midas_data(ENDPOINTS[endpoint])
        if data.status_code == 200:
            with open(os.path.join("midas-data", endpoint + ".json"), "w") as outfile:
                json.dump(data.json(), outfile, indent=4)


if __name__ == '__main__':
    main()
