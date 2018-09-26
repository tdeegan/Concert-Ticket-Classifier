import requests
import json
import pandas as pd
import numpy as np
import datetime
pd.set_option('display.max_columns', 500)

df = pd.DataFrame()

city_list = ['San Francisco', 'Oakland', 'Berkeley', 'San Jose',
            'New York', 'Brooklyn', 'Bronx', 'Flushing', 'East Rutherford',
           'Washington, DC', 'Vienna',
            'Chicago', 'Rosemont', 'Evanston',
           'Los Angeles', 'Hollywood', 'West Hollywood', 'Pasadena',
           'Boston', 'Medford']

for i in range(1,11):
    responseEvents = requests.get(f'https://api.seatgeek.com/events?per_page=5000&page={i}&client_id=MTI5NzU0MTd8MTUzNTkwMTU5Mi45Nw')

    eventsDict = json.loads(responseEvents.content.decode("utf-8")) #dictionary

    eventsList = eventsDict['events'] #list of ten dictionaries

    local_date = [eventsList[i]['datetime_local'] for i in range(len(eventsList))]
    utc_date = [eventsList[i]['datetime_utc'] for i in range(len(eventsList))]
    eventID = [eventsList[i]['id'] for i in range(len(eventsList))]
    score = [eventsList[i]['score'] for i in range(len(eventsList))]
    title = [eventsList[i]['short_title'] for i in range(len(eventsList))]
    avgPrice = [eventsList[i]['stats']['average_price'] for i in range(len(eventsList))]
    hiPrice = [eventsList[i]['stats']['highest_price'] for i in range(len(eventsList))]
    listCount = [eventsList[i]['stats']['listing_count'] for i in range(len(eventsList))]
    lowPrice = [eventsList[i]['stats']['lowest_price'] for i in range(len(eventsList))]
    lowPriceDeals = [eventsList[i]['stats']['lowest_price_good_deals'] for i in range(len(eventsList))]
    Type = [eventsList[i]['type'] for i in range(len(eventsList))]
    lat = [eventsList[i]['venue']['location']['lat'] for i in range(len(eventsList))]
    lon = [eventsList[i]['venue']['location']['lon'] for i in range(len(eventsList))]
    postal = [eventsList[i]['venue']['postal_code'] for i in range(len(eventsList))]
    url = [eventsList[i]['url'] for i in range(len(eventsList))]
    performer = [eventsList[i]['performers'][0]['name'] for i in range(len(eventsList))]
    venueCity = [eventsList[i]['venue']['city'] for i in range(len(eventsList))]
    venueState = [eventsList[i]['venue']['state'] for i in range(len(eventsList))]
    venueName = [eventsList[i]['venue']['name'] for i in range(len(eventsList))]
    venueID = [eventsList[i]['venue']['id'] for i in range(len(eventsList))]

    dfNew = pd.DataFrame({"local_date": local_date, "utc_date": utc_date, "eventID": eventID, "score": score, "title":title, 
    	"avgPrice": avgPrice, "hiPrice": hiPrice, "listCount": listCount, "lowPrice": lowPrice, 
    	"lowPriceDeals": lowPriceDeals, "Type":Type, "lat": lat, "lon": lon, "postal":postal, 
    	"url": url, "performer": performer, "venueCity": venueCity, "venueState": venueState,
    	"venueName": venueName, "venueID": venueID})
    
    df = pd.concat([df, dfNew])

today = datetime.date.today()

df = df.loc[df.venueCity.apply(lambda x: x in city_list)] #filter by city list
df = df.loc[df.Type == "concert"] #filter by Type == concert

eventsDF = df[['eventID','local_date','performer','title','utc_date', 'venueID']].drop_duplicates(subset = 'eventID')

venueDF = df[['venueID','venueName','venueCity','venueState','postal','lat','lon']].drop_duplicates(subset = 'venueID')

pricesDF = df[['eventID','avgPrice','hiPrice','listCount',
               'lowPrice','lowPriceDeals','score']].groupby('eventID').mean()

pricesDF = pricesDF[np.sum(pricesDF.isnull(), axis = 1) != 5].reset_index()
pricesDF['accessDate'] = today
pricesDF['accessTime'] = pd.Timestamp.now()
eventsDF['accessTime'] = pd.Timestamp.now()
venueDF['accessTime'] = pd.Timestamp.now()

eventsDF.to_csv(f'SeatGeek_eventsDF_{today.month}_{today.day}_{today.year}.csv', index = False) #accessTime
venueDF.to_csv(f'SeatGeek_venuesDF_{today.month}_{today.day}_{today.year}.csv', index = False) #accessTime
pricesDF.to_csv(f'SeatGeek_pricesDF_{today.month}_{today.day}_{today.year}.csv', index = False) #accessTime







