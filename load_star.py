'''
Created on 03-Nov-2015

@author: Jim D'Souza

Description : Import twitter data (stored in json format) from MongoDB
'''

from pymongo import MongoClient
import json
from pprint import pprint
import datetime

from os import listdir
from os.path import isfile, join

start_time = datetime.datetime.now()
print "Starting time : ", start_time

def iter_recursive(d):
    for k, v in d.items():
        if isinstance(v, dict):
            for k1, v1 in iter_recursive(v):
                yield (k,) + k1, v1
        else:
            yield (k,), v


data_source = "Quora"

if data_source == "Twitter" :

    #data_dir = '/mnt/datafolder/JSON Files/'
    data_dir = "D:\\StarTV\\Data\\JSON Files\\new\\"

    client = MongoClient()
    db = client.twitter
    collection = db.star

    json_files = [ f for f in listdir(data_dir) if isfile(join(data_dir,f)) ]

    for filename in json_files :
        print filename," start time : ", datetime.datetime.now() - start_time
        bad_recs = 0
        for i, line in enumerate(open(data_dir + filename, 'r')):
            try:
                json_obj = json.loads(line.strip())
                collection.insert_one(json_obj)
            except ValueError:
                bad_recs += 1

            if (i+1) % 1000 == 0:
                print('Completed: {0:.2%}'.format(float(i + 1) / 2998444))

            print("# bad records skipped: {}".format(bad_recs))

elif data_source == "Quora" :

    #data_dir = '/mnt/datafolder/JSON Files/'
    data_dir = "D:\\StarTV\\Data\\Quora Files\\"

    client = MongoClient()
    db = client.quora
    collection = db.star

    csv_files = [ f for f in listdir(data_dir) if isfile(join(data_dir,f)) ]

    for fn in csv_files :
        filename = fn.strip('A_Quora_MasterSheet_').strip('.csv')
        print filename," start time : ", datetime.datetime.now() - start_time
        
        topics = filename.split("+_+")
        for tp in topics :
            print tp.replace('+',' ').strip()
        
        """
        for i, line in enumerate(open(data_dir + filename, 'r')):
            try:
                json_obj = json.loads(line.strip())
                collection.insert_one(json_obj)
            except ValueError:
                bad_recs += 1

            if (i+1) % 1000 == 0:
                print('Completed: {0:.2%}'.format(float(i + 1) / 2998444))

            print("# bad records skipped: {}".format(bad_recs))
        """