import json
import os.path

import requests

BASE_URL = "https://catalog.midasnetwork.us/midas-viz-api/"

ENDPOINTS = {
    "ORGANIZATION_HIERARCHY_ENDPOINT": "orgHierarchy/"
}

VIZ_OUTPUT_DIR = ""
MIDAS_API_OUTPUT_DIR = ""


##make function to read APIKEY from a file

def fetch_visualizer_data(endpoint):
    return requests.get(BASE_URL + endpoint)


def main():
    for endpoint in ENDPOINTS:
        data = fetch_visualizer_data(ENDPOINTS[endpoint])
        with open(os.path.join(MIDAS_API_OUTPUT_DIR, endpoint + ".json"), "w", encoding="utf-8") as outfile:
            json.dump(data.json(), outfile, indent=4, ensure_ascii=True)


if __name__ == '__main__':
    main()
