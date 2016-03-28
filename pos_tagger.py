'''
Created on 09-Nov-2015

@author: Jim D'Souza

Description : Uses TextBlob to identify parts of speech of a tweet
'''

import string
from textblob import Blobber
from textblob_aptagger import PerceptronTagger

import csv
import datetime

windows_path = "D:\\StarTV\\LDA Model\\Output\\age_csv.csv"
azure_path = "/mnt/datafolder/Output/age_csv.csv"

age_file = azure_path

def main():
    
    start_time = datetime.datetime.now()
    
    print "Starting time : ", start_time
    
    ### Import text
    ### corpus 1
    user_dictionary = {}
    rowcount = 0
    with open(age_file, 'rb') as f:
        reader = csv.reader(f)
        for row in reader:
            if rowcount == 0 :
                rowcount += 1
            else :
                if row[0] not in user_dictionary :
                    user_dictionary[row[0]] = []
                user_dictionary[row[0]].append(row[1])
    
    print "Corpus import time : ", datetime.datetime.now() - start_time
    

    tb = Blobber(pos_tagger=PerceptronTagger())
    for user in user_dictionary :
        for tweet in user_dictionary[user] :
            print tweet
            
            # Get the untagged sentence string
            blob = tb(tweet)
            
            # tagger excludes punctuation by default
            for word, tag in blob.tags :
                print word, tag
    
    print "POS Tagging import time : ", datetime.datetime.now() - start_time

if __name__ == '__main__':
    main()