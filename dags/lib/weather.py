import requests


def get_daily_weather():
    url = "https://odre.opendatasoft.com/api/records/1.0/search/?dataset=temperature-quotidienne-regionale&q=&rows=-1&facet=date&facet=region"
    headers = {
        "accept": 'application/json',
    }
    response = requests.get(url, headers=headers)
    return response.json()

