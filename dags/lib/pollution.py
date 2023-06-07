import requests
import calendar
import time
from dotenv import load_dotenv
import os


def get_pollution():
    lat = "48.866667"
    long = "2.333333"
    current_GMT = time.gmtime()
    end_date = str(calendar.timegm(current_GMT))
    load_dotenv()
    open_weather_api_key = os.getenv("OPEN_WEATHER_API")
    print(open_weather_api_key)
    url = "https://api.openweathermap.org/data/2.5/air_pollution/history?lat=" + lat + "&lon=" + long + "&start=1577887236&end=" + end_date + "&appid=" + open_weather_api_key
    headers = {
        "accept": 'application/json',
    }

    response = requests.get(url, headers=headers)
    return response.json()
