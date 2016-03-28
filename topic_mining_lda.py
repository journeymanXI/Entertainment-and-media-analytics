'''
Created on 21-Oct-2015

@author: Jim DSouza

Description : Main code which reads in the data set created by create_ngram_datset.py
and uses LDA to create topics out of the tweet text.
The topics are numbered 1-100 and are assigned to each tweet, and stored in the MongoDB
'''

import csv
import datetime
import re

from gensim import corpora
from gensim.models import ldamodel
from gensim.models import lsimodel

import numpy as np
import lda
import nltk
from nltk.stem.snowball import SnowballStemmer

from pymongo import MongoClient


windows_path = "D:\\StarTV\\LDA Model\\"
azure_path = "/mnt/datafolder/Output/"

ngram_uni_file = windows_path+"Output\\ngrams_uni.csv"
ngram_bi_file = windows_path+"Output\\ngrams_bi.csv"
city_file = windows_path + "Output\\cities_coordinates.csv"
model_dataset = windows_path+"Output\\model_dataset.csv"
model_output = windows_path+"Output\\model_output.csv"

stopWord_file = windows_path + "Model Datasets/Stopwords.csv"
age_input_3 = windows_path + "Output\\age_prediction_3.csv"

start_time = datetime.datetime.now()
print "Start time : ", start_time

##### Import Sample corpus

def processText(text,stopWords,punctuations):
    ### Manipulating text
    'z'
    #Convert to lower case
    text = text.lower()
    
    ### Remove stop words
    for sw in stopWords :
        text= re.sub(r"\b%s\b" % sw ,'', text)

    ### Remove punctuation
    for pt in punctuations :
        text= text.replace(pt,' ')

    #Remove additional white spaces
    text = " ".join(text.split())
    
    #trim
    text = text.strip('\'"')
    return text


def generateTopics(corpus, dictionary):
    # Build LDA model using the above corpus
    
    #lda = ldamodel.LdaModel(corpus, id2word=dictionary, num_topics=100)
    #corpus_lda = lda[corpus]
    lda = lsimodel.LsiModel(corpus, id2word=dictionary, num_topics=100)
    corpus_lda = lda[corpus]

    # Group topics with similar words together.
    tops = set(lda.show_topics(num_topics=100,num_words=20))
    top_clusters = []
    for l in tops:
        top = []
        for t in l.split(" + "):
            top.append((t.split("*")[0], t.split("*")[1]))
        top_clusters.append(top)

    # Generate word only topics
    top_wordonly = []
    for i in top_clusters:
        top_wordonly.append(":".join([j[1] for j in i]))

    return lda, corpus_lda, top_clusters, top_wordonly   


# Finding ngrams in the text
def find_ngrams(input_string,n):
    ngrams = nltk.ngrams(input_string.split(),n)
    return ngrams

def main(method):
    
    # Start connection
    
    client = MongoClient()
    db = client.twitter
    collection = db.star
    
    #collection.create_index('postedTime', unique=True)
    
    print "Connection established : ", datetime.datetime.now() - start_time
    
    ### Reading Stopwords file
    stopWords = []
    with open(stopWord_file, 'rb') as f:
        reader = csv.reader(f)
        for row in reader:
            stopWords.append(row[0].lower())
            
    
    ### Reading Coordinates file
    # Zones are as follows :
    # Delhi
    # Mumbai
    # Kolkata
    # Chennai
    # Bangalore
    # Hyderabad
    # South (TN+Kerala+Karnataka+AP)
    # West (Gujarat+Rajasthan+Maharashtra+Goa)
    # Central (MP+Chattisgarh)
    # North (UP+Bihar+Jharkhand+Haryana+Punjab+Himachal+Uttaranchal)
    # East (West Bengal+Orissa)
    # North East (7 sisters)
    # J&K

    zone_city = []
    zone = "North"
    rowcount = 0
    with open(city_file, 'rb') as f:
        reader = csv.reader(f)
        for row in reader:
            if rowcount <=1 :
                rowcount = rowcount + 1
            else :
                if row[7].lower() == zone.lower() :
                    zone_city.append(row[0].lower())
            
    print "Reading stopwords file : ", datetime.datetime.now() - start_time
    
    ### Reading Predicted Users file
    Predicted_users = []
    with open(age_input_3, 'rb') as f:
        reader = csv.reader(f)
        for row in reader:
            if row[6] != "" :
                Predicted_users.append(row[0])
                
    
    ### Reading NGrams file
    ngrams_uni = {}
    rowcount = 0
    with open(ngram_uni_file, 'rb') as f:
        reader = csv.reader(f)
        for row in reader:
            if rowcount == 0 :
                rowcount = rowcount + 1
            else :
                if row[2] == "1" :
                    if row[0] not in ngrams_uni :
                        ngrams_uni[row[0]] = int(row[1])
    ngrams_bi = {}
    rowcount = 0
    with open(ngram_bi_file, 'rb') as f:
        reader = csv.reader(f)
        for row in reader:
            if rowcount == 0 :
                rowcount = rowcount + 1
            else :
                if row[2] == "1" :
                    if row[0] not in ngrams_bi :
                        ngrams_bi[row[0]] = int(row[1])
            
    print "Reading ngrams file : ", datetime.datetime.now() - start_time
    
    rowcount = 0
    ### Sample corpus 1
    
    #print "Finding data time : ", datetime.datetime.now() - start_time
    
    user_dictionary = {}  
    pattern = re.compile('[\W_]+')
    count = 0

    query = collection.find({"month" : 4}).limit(10000)
    
    print "Reading query : ", datetime.datetime.now() - start_time
    
    for docs in query :
        try :
            print count
            count = count + 1
            user = docs['actor']['preferredUsername'].encode('ascii', 'ignore')
            doc_id = docs["_id"]

            #if user in Predicted_users and (location.lower() in zone_city):
            if user in Predicted_users:
            
                tweet = docs['object']['summary'].encode('ascii', 'ignore')
        
                # Removing punctuations 
                tweet = pattern.sub(' ', tweet)
        
                # Removing stopwords
                tweet_text = ""
                tweet_1 = ""
                tweet_2 = []

                for word in tweet.lower().split() :
                    #if word not in stopWords
                    if word not in stopWords:
                        tweet_1 = tweet_1 + " " + re.sub(r'(.)\1+', r'\1\1', word)
                
                bigrams = find_ngrams(tweet_1,2)
                for bg in bigrams :
                    if bg[0]+" "+bg[1] in ngrams_bi :
                        tweet_text = tweet_text + " "+ bg[0]+"-"+bg[1]
                        #tweet_2.append(bg[0])
                        #tweet_2.append(bg[1])
                     
                for word in tweet.lower().split() :
                    if word in ngrams_uni:
                    #if word in ngrams_uni and word not in tweet_2:
                        tweet_text = tweet_text + " " + re.sub(r'(.)\1+', r'\1\1', word)
                
                if user not in user_dictionary :
                    user_dictionary[user] = {}
                    user_dictionary[user]["tweets"] = []
                    user_dictionary[user]["doc_id"] = []

                user_dictionary[user]["tweets"].append(tweet_text)
                user_dictionary[user]["doc_id"].append(doc_id)

        except :
            print "Skipping..."
            
    #client.close()      
    print "Dataset 1 imported to dictionary : ", datetime.datetime.now() - start_time
    
    tweet_dataset = []
    doc_ids = []
    for user in user_dictionary :
        for tweet in user_dictionary[user]["tweets"] :
            tweet_dataset.append(tweet)
        for doc_id in user_dictionary[user]["doc_id"] :
            doc_ids.append(doc_id)
    
    print "Dataset moved to list : ", datetime.datetime.now() - start_time
    
    # Keeping 10000 random tweets for trial
    #random.shuffle(tweet_dataset)
    #tweet_dataset = tweet_dataset[:100000]

    
    texts = [[word  for word in tweet.lower().split()] for tweet in tweet_dataset]
    
    
    dictionary = corpora.Dictionary(texts)
    corpus = [dictionary.doc2bow(text) for text in texts]
    
    print "Text Import Run Time : ", datetime.datetime.now() - start_time
           
    if method == 1 :
        ##### Train the LDA Model
        lda, corpus_lda, topic_clusters, topic_wordonly = generateTopics(corpus, dictionary)
        
        writer = csv.writer(open(model_output, 'wb'))
        header = ["Cluster","Word","Probability"]
        writer.writerow(header)
        for i in range(len(topic_clusters)) :
            print "Cluster : ", topic_clusters[i]
            for tc in topic_clusters[i] :
                row = []
                row.append(i+1)
                row.append(tc[1])
                row.append(tc[0])
                writer.writerow(row)
                
        # Start second connection
    
        #client = MongoClient()
        #db = client.twitter
        #collection = db.star
        
        #for i in range(len(doc_ids)) :
        #    print i
        #    lda_result = corpus_lda[i]
        #    collection.update({'_id':doc_ids[i]}, {"$set" : {"topics": lda_result}}, upsert=False)
    
    if method == 2 :
        
                
        ### Sample corpus 1
        X = np.loadtxt(open(model_dataset,"rb"),dtype="int",delimiter=",",skiprows=1)
        #X = X[:,1:]
        print "type(X): {}".format(type(X))
        print "shape: {}\n".format(X.shape)
        
        # the vocab
        vocab = []
        rowcount = 0
        with open(ngram_uni_file, 'rb') as f:
            reader = csv.reader(f)
            for row in reader:
                if rowcount == 0 :
                    rowcount = rowcount + 1
                else :
                    if int(row[1]) >= 20 and row[2] == "1":
                        if row[0] not in vocab :
                            vocab.append(row[0])
                    
                            
        print "type(vocab): {}".format(type(vocab))
        print "len(vocab): {}\n".format(len(vocab))
        
        # Fit the model
        
        print "Fitting the model..."
        model = lda.LDA(n_topics=50, n_iter=1000, random_state=1)
        model.fit(X)
        
        
        # Document topic
        
        topic_word = model.topic_word_
        print "type(topic_word): {}".format(type(topic_word))
        print "shape: {}".format(topic_word.shape)
                    
        n = 30
        for i, topic_dist in enumerate(topic_word):
            topic_words = np.array(vocab)[np.argsort(topic_dist)][:-(n+1):-1]
            print '*Topic {}\n- {}'.format(i, ' '.join(topic_words))
            
        doc_topic = model.doc_topic_
        print "type(doc_topic): {}".format(type(doc_topic))
        print "shape: {}".format(doc_topic.shape)
    
        for n in range(5):
            sum_pr = sum(doc_topic[n,:])
            print "document: {} sum: {}".format(n, sum_pr)
            
        for n in range(10):
            topic_most_pr = doc_topic[n].argmax()
            print "doc: {} topic: {}\n{}...".format(n,topic_most_pr)
    
    print "LDA Run Time : ", datetime.datetime.now() - start_time
    
if __name__ == '__main__':
    method = 1
    main(method)

