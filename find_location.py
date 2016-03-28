'''
Created on 21-Oct-2015

@author: Jim D'Souza

Description : This code extracts the user location from where the tweet originated
'''

### Import relevant libraries
import csv
import datetime
import re

from gensim import corpora
from gensim.models import ldamodel

import numpy as np
import lda

from pymongo import MongoClient


### Change path based on the type of system being used ###
windows_path = "D:\\StarTV\\LDA Model\\"
azure_path = "/mnt/datafolder/Output/"

city_file = windows_path + "Output\\cities_coordinates.csv"
location_dataset = windows_path + "Output\\locations.csv"

start_time = datetime.datetime.now()
print "Start time : ", start_time

def main():
    
    # Start connection
    
    client = MongoClient()
    db = client.twitter
    collection = db.star
    
    #collection.create_index('postedTime', unique=True)
    
    print "Connection established : ", datetime.datetime.now() - start_time
    
    ### Reading Coordinates file
    city_coordinates = {}
    rowcount = 0
    with open(city_file, 'rb') as f:
        reader = csv.reader(f)
        for row in reader:
            if rowcount <=1 :
                rowcount = rowcount + 1
            else :
                city_coordinates[row[0]] = []
                city_coordinates[row[0]].append(float(row[1]))
                city_coordinates[row[0]].append(float(row[2]))
            
    print "Reading city coordinates file : ", datetime.datetime.now() - start_time
    
    rowcount = 0
    ### Sample corpus 1
    
    #print "Finding data time : ", datetime.datetime.now() - start_time
    
    user_dictionary = {}  
    pattern = re.compile('[\W_]+')
    count = 0
        
    query = collection.find()
    #writer = csv.writer(open(location_dataset, 'wb'))
    #header = ["User","Location"] 
    #writer.writerow(header)
    rowcount = 0
    for docs in query :
        rowcount = rowcount + 1
        print rowcount
        try :
            id = docs["_id"]
            month = int(docs["postedTime"][5:7])
            collection.update({'_id':id}, {"$set" : {"month": month}}, upsert=False)
            
            user = docs['actor']['preferredUsername'].encode('ascii', 'ignore')
            location = docs['user_location']
            if user not in user_dictionary :
                user_dictionary[user] = location
                row = []
                row.append(user)
                row.append(location)
                writer.writerow(row)
            
            
            #location = docs['actor']['location']['displayName'].lower()
            try :
                location = docs['gnip']['profileLocations'][0]['address']['locality'].encode('ascii', 'ignore')
            except :
                location = docs['gnip']['matching_rules'][0]['value']
                lat = float(location[location.find("[")+1:location.find(" ")])
                lon = location[location.find(" ")+1:location.find("]")]
                lon = float(lon[:lon.find(" ")])
                for city in city_coordinates :
                    if lat == city_coordinates[city][0] and lon == city_coordinates[city][1] :
                        location = city
            
            
            collection.update({'_id':id}, {"$set" : {"user_location": location}}, upsert=False)
            
        except :
            print "Skipping user"

    
if __name__ == '__main__':
    main()

