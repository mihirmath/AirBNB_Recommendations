from amadeus import Client
from neo4j import GraphDatabase
from geopy.geocoders import Nominatim

lat = 37.7749
long = -122.4194

amadeus = Client(
    client_id='MODEiG7lXARU6ZVBI5SeGUgbKLXq5UBe',
    client_secret='gh0etd0dSTnZ7AVG'
)
activityResponse = amadeus.shopping.activities.get(
            latitude=lat, # + --> North, - --> South
            longitude=long) # + --> East, - --> West

def usd(amount):
    usd_amount = float(amount) * 1.18
    return round(usd_amount, 2)

#Local DB
#url = "bolt://localhost:7687"
#driver = GraphDatabase.driver(url, auth=("neo4j", "neo4j123"))

#Cloud DB
url = "neo4j+s://e54715b3.databases.neo4j.io:7687"
driver = GraphDatabase.driver(url, auth=("neo4j", "d6xX8PrwU_0UMPhqAy76MMMiuAtzJqF6_djE3TnliO0"))

def merge_activity_node(tx, id, name, description, price, rating, bookingLink, pictureURL, coordinates, city):
    #Query
    tx.run('MERGE (activity:Activity {id: $id})'
           'SET activity.name=$name, activity.description=$description, activity.price=$price, activity.rating=$rating, activity.bookingLink=$bookingLink, activity.pictureURL=$pictureURL '
           'MERGE (coordinates:Coordinates {value: $coordinates})'
           'MERGE (city:City {name: $city})'
           'MERGE (activity)-[:HAS_LOCATION]->(coordinates)'
           'MERGE (coordinates)-[:IN_CITY]->(city)', id=id, name=name, description=description, price=price, rating=rating, 
                                                     bookingLink=bookingLink, pictureURL=pictureURL, coordinates=coordinates,
                                                     city=city)
for i in range(len(activityResponse.data)):
    id = activityResponse.data[i]['id']
    name = activityResponse.data[i]['name']
    description = activityResponse.data[i]['shortDescription']
    if activityResponse.data[i]['price']['currencyCode'] == 'EUR':
        price = usd(float(activityResponse.data[i]['price']['amount']))
    else:
        price = float(activityResponse.data[i]['price']['amount'])
    rating = activityResponse.data[i]['rating']
    bookingLink = activityResponse.data[i]['bookingLink']
    pictureURL = activityResponse.data[i]['pictures'][0]
    latitude = activityResponse.data[i]['geoCode']['latitude']
    longitude = activityResponse.data[i]['geoCode']['longitude']
    coordinates = str(latitude) + ',' + str(longitude)
    geolocator = Nominatim(user_agent="loadPointsofInterest.py")
    location = geolocator.reverse(str(latitude) + ', ' + str(longitude))
    city = location.raw['address']['city']
    with driver.session() as session:
        result = session.write_transaction(merge_activity_node, id=id, name=name, description=description, price=price, 
                                                                rating=rating, bookingLink=bookingLink, pictureURL=pictureURL, 
                                                                coordinates=coordinates, city=city)
