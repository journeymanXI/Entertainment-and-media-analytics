'''
Created on 03-Nov-2015

@author: Jim D'Souza

Description : Using the n-grams created from ngrams_age_buckets.py, we create dummy variables and add them to the data set
The data set is used later by an LDA clustering algorithm to find relevant topics within the text
'''

import csv
import re
from collections import Counter
from topic_mining_lda import start_time
import datetime

from pymongo import MongoClient

#windows_path = "D:\\StarTV\\LDA Model\\Output\\"
windows_path = "E:\\Python\\Star\\"
azure_path = "/mnt/datafolder/"

ngram_file = windows_path+"ngrams_uni.csv"
model_dataset = windows_path+"model_dataset.csv"


def main():
    
    start_time = datetime.datetime.now()
    print "Starting time : ", start_time
    
    # Start connection
    
    client = MongoClient()
    db = client.twitter
    collection = db.star
    
    ### Sample corpus 1
    ngrams = {}
    rowcount = 0
    with open(ngram_file, 'rb') as f:
        reader = csv.reader(f)
        for row in reader:
            if rowcount == 0 :
                rowcount = rowcount + 1
            else :
                if int(row[1]) >= 1900 :
                    if row[0] not in ngrams :
                        ngrams[row[0]] = row[1]
    
    print "Imported ngrams : ", datetime.datetime.now() -start_time
    
    user_dictionary = {}
    rowcount = 0
    user_dictionary = {}  
    pattern = re.compile('[\W_]+')
    count = 0

    query = collection.find({"month" : 4}).limit(2000)
    
    for docs in query :
        try :
            print rowcount
            rowcount = rowcount + 1
            
            user_dictionary[rowcount] = {}
                
            text = docs['object']['summary'].encode('ascii', 'ignore')
            text = pattern.sub(' ', text)
            
            counter = Counter(word.lower() for word in re.findall(r"\w+", text))
            for word in counter :
                word_count = counter[word]
                if word in ngrams :
                    user_dictionary[rowcount][word] = word_count
        
        except :
            print "Skipping row"
    
    print "Imported tweets : ", datetime.datetime.now() -start_time
    
    writer = csv.writer(open(model_dataset, 'wb'))
    header = ["Tweet Number"]
    for word in ngrams :
        header.append(word)
    writer.writerow(header)
    rowcount = 0
    for rownumber in user_dictionary :
        print rowcount
        rowcount = rowcount + 1
        row = []
        row.append(rownumber)
        for word in ngrams :
            try:
                row.append(user_dictionary[rowcount][word])
            except :
                row.append(0)
        writer.writerow(row)
    
    print "Written to output : ", datetime.datetime.now() -start_time

if __name__ == '__main__':
    main()