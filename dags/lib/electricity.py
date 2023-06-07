import requests


def get_electricity_consumption():
    url = "https://odre.opendatasoft.com/api/records/1.0/search/?dataset=consommation-maximale-horaire-de-gaz-par-jour-a-la-maille-regional&q=&rows=-1&facet=date&facet=nom_officiel_region&facet=code_officiel_region"
    headers = {
        "accept": 'application/json',
    }
    response = requests.get(url, headers=headers)
    return response.json()

