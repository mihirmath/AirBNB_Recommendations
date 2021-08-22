from amadeus import Client
from neo4j import GraphDatabase
from geopy.geocoders import Nominatim

lat = 37.7749
long = -122.4194

amadeus = Client(
    client_id='MODEiG7lXARU6ZVBI5SeGUgbKLXq5UBe',
    client_secret='gh0etd0dSTnZ7AVG'
)
interestResponse = amadeus.reference_data.locations.points_of_interest.get(
            latitude=lat, # + --> North, - --> South
            longitude=long) # + --> East, - --> West

#Local DB
#url = "bolt://localhost:7687"
#driver = GraphDatabase.driver(url, auth=("neo4j", "neo4j123"))

#Cloud DB
url = "neo4j+s://e54715b3.databases.neo4j.io:7687"
driver = GraphDatabase.driver(url, auth=("neo4j", "d6xX8PrwU_0UMPhqAy76MMMiuAtzJqF6_djE3TnliO0"))

def usd(amount):
    usd_amount = float(amount) * 1.18
    return round(usd_amount, 2)

def merge_pointOfInterest_node(tx, id, name, rank, tag, category, coordinates, city):
    #Query
    tx.run('MERGE (poi:`Point of Interest` {id: $id})'
           'SET poi.name = $name, poi.rank = $rank '
           'MERGE (tag:Tag {name: $tag})'
           'MERGE (category:Category {name: $category})'
           'MERGE (coordinates:Coordinates {value: $coordinates})'
           'MERGE (city:City {name: $city})'
           'MERGE (poi)-[:IN_CATEGORY]->(category)'
           'MERGE (poi)-[:HAS_TAG]->(tag)'
           'MERGE (poi)-[:HAS_LOCATION]->(coordinates)'
           'MERGE (coordinates)-[:IN_CITY]->(city)', id=id, name=name, rank=rank, tag=tag, category=category, 
                                                     coordinates=coordinates, city=city)
for i in range(len(interestResponse.data)):
    id = interestResponse.data[i]['id']
    name = interestResponse.data[i]['name']
    rank = interestResponse.data[i]['rank']
    category = interestResponse.data[i]['category']
    latitude = interestResponse.data[i]['geoCode']['latitude']
    longitude = interestResponse.data[i]['geoCode']['longitude']
    coordinates = str(latitude) + ',' + str(longitude)
    geolocator = Nominatim(user_agent="loadPointsofInterest.py")
    location = geolocator.reverse(str(latitude) + ', ' + str(longitude))
    city = location.raw['address']['city']
    for x in range(len(interestResponse.data[i]['tags'])):
        tag = interestResponse.data[i]['tags'][x]
        with driver.session() as session:
            result = session.write_transaction(merge_pointOfInterest_node, id, name, rank, tag, category, coordinates, city)