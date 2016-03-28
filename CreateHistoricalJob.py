'''
Created on 30-Oct-2015

@author: Jim D'Souza

Description : The csv files were given in a zipped format.
This code unzips and extracts the files, and creates new csv files and adds tweets to them.
This reduces the number of csv files.
Multi-threading is used to speed up the process
'''
#!/usr/bin/env python

import urllib
import base64
import json
import sys
import csv
import gzip
import time

import threading
import multiprocessing

iid_lock = threading.Lock()

file = "D:\\StarTV\\Data\\results.csv"
file_path_zip = "D:\\StarTV\\Data\\Zipped Files\\"
file_path_json = "D:\\StarTV\\Data\\JSON Files\\"


def fetch_url(name,link) :
    try :
        try:
            print name
            urllib.urlretrieve(link, file_path_zip + name)
    
        except Exception:
            print "Missed a URL"
            with iid_lock:
                errfile = open(file_path_zip + "err.txt", "a")
                errfile.write(name+",,"+link+"\n")
                errfile.close()
 
    
        inF = gzip.open(file_path_zip + name, 'rb')
            
        data = []
        for row in inF :
            data.append(row)
            
        with open(file_path_json + name.strip(".gz"), 'wb') as outF:
            for row in data :
                if row != "" or row != "\n":
                    outF.write(row)
            
        inF.close()
        outF.close()
    
    except :
        print "error"

def post():
    
    file_dict = {}
    # read csv
    with open(file, 'rb') as f:
        reader = csv.reader(f)
        for row in reader:
            if row[1] == "" :
                file_dict[row[0]] = row[2]


    maximumNumberOfThreads = 100
    #threads = [threading.Thread(target=fetch_url, args=(name,link,)) for name,link in file_dict.iteritems()]

    thread_counter = 0
    for name, link in file_dict.iteritems() :
        
        if thread_counter < maximumNumberOfThreads :
            thread_counter += 1
            thread = threading.Thread(target=fetch_url, args=(name,link,))
            thread.start()
            print thread_counter
            
        if thread_counter == maximumNumberOfThreads : 
            thread_counter = 0
            thread.join()         
    

if __name__ == "__main__":

    post()