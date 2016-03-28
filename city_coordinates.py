'''
Created on 14-Oct-2015

@author: Jim D'Souza

Description : We have an initial list of cities for which we want to obtain tweets.
This code finds the co-ordinates of each city in this list.
'''
import csv
import time

from geopy.geocoders import Nominatim

city_file = "D:\\StarTV\\cities.csv"
output_file = "D:\\StarTV\\cities_coordinates.csv"

city_list = []
with open(city_file,'rb') as file:
    contents = csv.reader(file)
    for x in contents :
        city_list.append(x[0])

location_list = []

for city in city_list :
    print city
    location_list_cd = []
    location_list_cd.append(city)
    try :
        geolocator = Nominatim()
        location = geolocator.geocode(city)
            
    except:
        print "Error in finding location - ",city,". Trying again."
        time.sleep(1)
        location = geolocator.geocode(city)
    
    try :
    
        location_list_cd.append(location.longitude)
        location_list_cd.append(location.latitude)
    
    
        location_bounds = location.raw['boundingbox']
        for i in range(len(location_bounds)) :
            location_bounds[i] = float(location_bounds[i])
                        
        location_bounds[0],location_bounds[1] = location_bounds[1],location_bounds[0]
        location_bounds[0],location_bounds[2] = location_bounds[2],location_bounds[0]
        location_bounds[2],location_bounds[3] = location_bounds[3],location_bounds[2]
            
        location_list_cd.append(location_bounds[0])
        location_list_cd.append(location_bounds[1])
        location_list_cd.append(location_bounds[2])
        location_list_cd.append(location_bounds[3])
    
        location_list.append(location_list_cd)
    
    except :
        print "Error in finding coordinates."
            

with open(output_file, 'wb') as op:
    writer = csv.writer(op, delimiter=',')
    writer.writerows(location_list)
    