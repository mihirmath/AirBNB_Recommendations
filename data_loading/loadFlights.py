#Import Libraries
from amadeus import Client
from neo4j import GraphDatabase
import csv

#Initialize Amadeus Client
amadeus = Client(
    client_id='MODEiG7lXARU6ZVBI5SeGUgbKLXq5UBe',
    client_secret='gh0etd0dSTnZ7AVG'
)
#Gather API Data based on Given Parameters
flightResponse = amadeus.shopping.flight_offers_search.get(
            originLocationCode='SFO',
            destinationLocationCode='BOS',
            departureDate='2021-10-01',
            adults='1')

#Local DB
#url = "bolt://localhost:7687"
#driver = GraphDatabase.driver(url, auth=("neo4j", "neo4j123"))

#Cloud DB
url = "neo4j+s://e54715b3.databases.neo4j.io:7687"
driver = GraphDatabase.driver(url, auth=("neo4j", "d6xX8PrwU_0UMPhqAy76MMMiuAtzJqF6_djE3TnliO0"))

#Convert Euros to US Dollar, round to 2 decimals
def usd(amount):
    usd_amount = float(amount) * 1.18
    return round(usd_amount, 2)

#Parses duration into integer for mins
def parseDuration(duration):
    if 'H' in duration:
        duration_hr = int(duration.replace('PT', '').split('H')[0])
    else:
        duration_min = int(duration.replace('PT', '').split('H')[0].replace('M', ''))
        return duration_min
    if 'M' in duration:
        duration_min = int(duration.replace('PT', '').split('H')[1].replace('M', ''))
        duration = (duration_hr * 60) + duration_min
    else:
        duration = duration_hr * 60
    return duration
#Neo4j Python Driver
def merge_flight_node1(tx, id, flightNumber, oneWay, grandTotal, duration, departure_iataCode, departure_airportName, 
                      arrival_iataCode, arrival_airportName, departure_cityName, arrival_cityName, departure_countryName, 
                      arrival_countryName, carrierCode, airlineName, departureTime, arrivalTime):
    #Query
    tx.run('MERGE (f:Flight {flightNumber: $flightNumber, duration: $duration, grandTotal: $grandTotal})'
           'SET f.id=$id, f.oneWay=$oneWay '
           'MERGE (departureAirport:Airport {iataCode: $departure_iataCode, name: $departure_airportName})'
           'MERGE (arrivalAirport:Airport {iataCode: $arrival_iataCode, name: $arrival_airportName})'
           'MERGE (departureCity:City {name: $departure_cityName})'
           'MERGE (arrivalCity:City {name: $arrival_cityName})'
           'MERGE (departureCountry:Country {name: $departure_countryName})'
           'MERGE (arrivalCountry:Country {name: $arrival_countryName})'
           'MERGE (Airline:Airline {code: $carrierCode, name: $airlineName})'
           'MERGE (f)-[:DEPARTES_FROM {departureTime: $departureTime}]->(departureAirport)'
           'MERGE (f)-[:ARRIVES_AT {arrivalTime: $arrivalTime}]->(arrivalAirport)'
           'MERGE (departureAirport)-[:IN_CITY]->(departureCity)'
           'MERGE (arrivalAirport)-[:IN_CITY]->(arrivalCity)'
           'MERGE (departureCity)-[:IN_COUNTRY]->(departureCountry)'
           'MERGE (arrivalCity)-[:IN_COUNTRY]->(arrivalCountry)'
           'MERGE (f)-[:HAS_AIRLINE]->(Airline)', 
                                                    id=id, 
                                                    flightNumber=flightNumber,
                                                    oneWay=oneWay,
                                                    grandTotal=grandTotal,
                                                    duration=duration,
                                                    departure_iataCode=departure_iataCode,
                                                    departure_airportName=departure_airportName,
                                                    arrival_iataCode=arrival_iataCode,
                                                    arrival_airportName=arrival_airportName,
                                                    departure_cityName=departure_cityName,
                                                    arrival_cityName=arrival_cityName,
                                                    departure_countryName=departure_countryName,
                                                    arrival_countryName=arrival_countryName,
                                                    carrierCode=carrierCode,
                                                    airlineName=airlineName,
                                                    departureTime=departureTime,
                                                    arrivalTime=arrivalTime)
def merge_flight_node2(tx, id, flightNumber, oneWay, grandTotal, duration, departure_iataCode, departure_airportName, 
                      arrival_iataCode, arrival_airportName, departure_cityName, arrival_cityName, departure_countryName, 
                      arrival_countryName, carrierCode, airlineName, departureTime, arrivalTime, originFlightID):
    #Query
    tx.run('MERGE (f:Flight {flightNumber: $flightNumber, duration:$duration})'
           'SET f.id=$id, f.oneWay=$oneWay, f.grandTotal=$grandTotal '
           'MERGE (arrivalAirport:Airport {iataCode: $arrival_iataCode, name: $arrival_airportName}) '
           'MERGE (departureAirport:Airport {iataCode: $departure_iataCode, name: $departure_airportName})'
           'MERGE (departureCity:City {name: $departure_cityName})'
           'MERGE (arrivalCity:City {name: $arrival_cityName})'
           'MERGE (departureCountry:Country {name: $departure_countryName})'
           'MERGE (arrivalCountry:Country {name: $arrival_countryName})'
           'MERGE (Airline:Airline {code: $carrierCode, name: $airlineName})'
           'MERGE (f)-[:DEPARTES_FROM {departureTime: $departureTime}]->(departureAirport)'
           'MERGE (f)-[:ARRIVES_AT {arrivalTime: $arrivalTime}]->(arrivalAirport)'
           'MERGE (departureAirport)-[:IN_CITY]->(departureCity)'
           'MERGE (arrivalAirport)-[:IN_CITY]->(arrivalCity)'
           'MERGE (departureCity)-[:IN_COUNTRY]->(departureCountry)'
           'MERGE (arrivalCity)-[:IN_COUNTRY]->(arrivalCountry)'
           'MERGE (f)-[:HAS_AIRLINE]->(Airline) '
           'WITH f '
           'MATCH (n:Flight) WHERE n.id = $originFlightID '
           'MERGE (n)-[:NEXT_FLIGHT]->(f) ', 
                                                id=id, 
                                                flightNumber=flightNumber,
                                                oneWay=oneWay,
                                                grandTotal=grandTotal,
                                                duration=duration,
                                                departure_iataCode=departure_iataCode,
                                                departure_airportName=departure_airportName,
                                                arrival_iataCode=arrival_iataCode,
                                                arrival_airportName=arrival_airportName,
                                                departure_cityName=departure_cityName,
                                                arrival_cityName=arrival_cityName,
                                                departure_countryName=departure_countryName,
                                                arrival_countryName=arrival_countryName,
                                                carrierCode=carrierCode,
                                                airlineName=airlineName,
                                                departureTime=departureTime,
                                                arrivalTime=arrivalTime,
                                                originFlightID=originFlightID)
#Open airport csv file
with open ('data_loading/airports.csv') as iataFile:
    reader = csv.DictReader(iataFile)
    airports = []
    #Creating airports dictionary
    for row in reader:
        airports.append({
            "name": row['airport'],
            "iataCode": row['iataCode'].split('/')[0],
            "city": row['location'].split(',')[0],
            "location": row['location'],
            "country": row['country']
        })

#Open airline csv file
with open ('data_loading/airlines.csv') as airlineFile:
    reader = csv.DictReader(airlineFile)
    airlines = []
    #Creating airlines dictionary
    for row in reader:
        airlines.append({
            "code": row['Code'],
            "name": row['Name']
        })
#Loops through all flight trips
for a, flight in enumerate(flightResponse.data):
    oneWay = flight['oneWay']   
    grandTotal = usd(flight['price']['grandTotal'])
    pathIDList = []
    #Loops through all flight itineraries
    for i, itinerary in enumerate(flight['itineraries']):
        #Loops through all flight segments
        for ii, segment in enumerate(itinerary['segments']):
            id = int(segment['id'])
            pathIDList.append(id)
            flightNumber = segment['carrierCode'] + segment['number']
            duration = parseDuration(segment['duration'])
            departure_iataCode = segment['departure']['iataCode']
            departureTime = segment['departure']['at']
            arrival_iataCode = segment['arrival']['iataCode']
            arrivalTime = segment['arrival']['at']
            carrierCode = segment['carrierCode']
            #Checks if airline was in csv and assigns the code and name of the airline
            for airline in airlines:
                if carrierCode == airline['code']:
                    airlineName = airline['name']
            #Checks if airport was in csv and assigns the name, city, and country of the airport
            for airport in airports:
                if departure_iataCode in airport['iataCode']:
                    departure_airportName = airport['name']
                    departure_cityName = airport['city']
                    departure_countryName = airport['country']
                elif arrival_iataCode in airport['iataCode']:
                    arrival_airportName = airport['name']
                    arrival_cityName = airport['city']
                    arrival_countryName = airport['country']
            if ii != 0: #If it is not the first segment
                originFlightID = id - 1
                with driver.session() as session:
                #Runs Query
                    result = session.write_transaction(merge_flight_node2, id, flightNumber, oneWay, grandTotal, duration,
                                                    departure_iataCode, departure_airportName, arrival_iataCode, 
                                                    arrival_airportName, departure_cityName, arrival_cityName, 
                                                    departure_countryName, arrival_countryName, carrierCode, airlineName, 
                                                    departureTime, arrivalTime, originFlightID)
            else: #If it is the first segment
                with driver.session() as session:
                    #Runs Query
                    result = session.write_transaction(merge_flight_node1, id, flightNumber, oneWay, grandTotal, duration,
                                                    departure_iataCode, departure_airportName, arrival_iataCode, 
                                                    arrival_airportName, departure_cityName, arrival_cityName, 
                                                    departure_countryName, arrival_countryName, carrierCode, airlineName, 
                                                    departureTime, arrivalTime)
#Closes the Neo4j Python Driver
driver.close()

#If not the last segment next flight ID = next flight ID + 1
#USE: Current Flight ID - 1 to find original flight <-- Doesn't apply to first segment
#Idea: have different drivers for if it is the first flight or not because if it is a first flight then it wont have an origin