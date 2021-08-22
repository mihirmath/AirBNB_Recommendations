import requests
from neo4j import GraphDatabase

auth = {
    'client_id': 'MODEiG7lXARU6ZVBI5SeGUgbKLXq5UBe',
    'client_secret': 'gh0etd0dSTnZ7AVG',
    'grant_type': 'client_credentials'
}

token = requests.post('https://test.api.amadeus.com/v1/security/oauth2/token', data=auth).json()

countryCode = 'US'

covid_res = requests.get(
  'https://test.api.amadeus.com/v1/duty-of-care/diseases/covid19-area-report?countryCode={}'.format(countryCode), 
  headers={ "accept": "application/json", "Authorization": '{} {}'.format(token['token_type'], token['access_token'])}
)

response = covid_res.json()['data']

#Local DB
#url = "bolt://localhost:7687"
#driver = GraphDatabase.driver(url, auth=("neo4j", "neo4j123"))

#Cloud DB
url = "neo4j+s://e54715b3.databases.neo4j.io:7687"
driver = GraphDatabase.driver(url, auth=("neo4j", "d6xX8PrwU_0UMPhqAy76MMMiuAtzJqF6_djE3TnliO0"))

def merge_covid_node(tx, countryName, areaPolicyStatus, covidInfectionRate, maskIsRequired, entryRestrictionBan, covidInfectionLevel):
    #Query
    tx.run('MERGE (country:Country {name:$countryName})'
           'SET country.areaPolicyStatus=$areaPolicyStatus, country.covidInfectionRate=$covidInfectionRate, country.maskIsRequired=$maskIsRequired, country.entryRestriction=$entryRestrictionBan '
           'MERGE (covidInfectionLevel:`Covid Infection Level` {level: $covidInfectionLevel})',
                                                                                     countryName=countryName,
                                                                                     areaPolicyStatus=areaPolicyStatus,
                                                                                     covidInfectionRate=covidInfectionRate, 
                                                                                     maskIsRequired=maskIsRequired,
                                                                                     entryRestrictionBan=entryRestrictionBan,
                                                                                     covidInfectionLevel=covidInfectionLevel)

if response['area']['areaType'] == 'Country':
    countryName = response['area']['name']
covidInfectionRate = response['diseaseInfection']['rate']
covidInfectionLevel = response['diseaseInfection']['level']
covidInfectionDate = response['diseaseInfection']['date']
maskIsRequired = response['areaAccessRestriction']['mask']['isRequired']
if response['areaAccessRestriction']['entry']['ban'] == 'Yes':
    entryRestrictionBan = True
elif response['areaAccessRestriction']['entry']['ban'] == 'no':
    entryRestrictionBan = False
areaPolicyStatus = response['areaPolicy']['status']
exitRestrictionBan = response['areaAccessRestriction']['exit']['isBanned']
with driver.session() as session:
    result = session.write_transaction(merge_covid_node, countryName=countryName, 
                                                         areaPolicyStatus=areaPolicyStatus,
                                                         covidInfectionRate=covidInfectionRate, 
                                                         maskIsRequired=maskIsRequired,
                                                         entryRestrictionBan=entryRestrictionBan, 
                                                         covidInfectionLevel=covidInfectionLevel)