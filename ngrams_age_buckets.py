'''
Created on 03-Nov-2015

@author: Jim D'Souza

Description : Finding out all the uni, bi and tri-grams in the text, and calculating their frequencies
'''

import csv
import nltk
import re
from numpy import string_

import datetime

windows_path = "D:\\StarTV\\LDA Model\\Output\\"
azure_path = "/mnt/datafolder/Output/"

file_path = windows_path

age_file = file_path+"age_csv.csv"
age_ngrams_file = file_path+"ngrams_tri.csv"

# Finding ngrams in the text
def find_ngrams(input_string,n):
    ngrams = nltk.ngrams(input_string.split(),n)
    return ngrams

def main():

    start_time = datetime.datetime.now()
    print "Starting time : ", start_time
    
    ### Import text
    ### corpus 1
    tweet_dictionary = {}
    rowcount = 0
    with open(age_file, 'rb') as f:
        reader = csv.reader(f)
        for row in reader:
            if rowcount == 0 :
                rowcount += 1
            else :
                tweet_dictionary[row[1]] = 0
    
    print "Corpus import time : ", datetime.datetime.now() - start_time
   
    # Finding ngrams in text and calculating frequencies
    unigrams = {}
    for tweet in tweet_dictionary :
        unigram = find_ngrams(tweet,3)
        for ug in unigram :
            ug_text = ug[0] + " " + ug[1] + " " + ug[2]
            if ug_text in unigrams :
                unigrams[ug_text] += 1
            else :
                unigrams[ug_text] = 1   
    
    writer = csv.writer(open(age_ngrams_file, 'wb'))
    writer.writerow(["strings","count"])
    
	# Output ngrams to file
    for string in unigrams : 
        row = []
        row.append(string)
        row.append(unigrams[string])  
        writer.writerow(row)
    
if __name__ == '__main__':
    main()