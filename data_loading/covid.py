import requests

auth = {
    'client_id': 'MODEiG7lXARU6ZVBI5SeGUgbKLXq5UBe',
    'client_secret': 'gh0etd0dSTnZ7AVG',
    'grant_type': 'client_credentials'
}

token = requests.post('https://test.api.amadeus.com/v1/security/oauth2/token', data=auth).json()

covid_res = requests.get(
  'https://test.api.amadeus.com/v1/duty-of-care/diseases/covid19-area-report?countryCode=US&cityCode=SFO', 
  headers={ "accept": "application/json", "Authorization": '{} {}'.format(token['token_type'], token['access_token'])}
)
print()
print(covid_res.json())