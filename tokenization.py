'''
Created on 21-Oct-2015

@author: Jim D'Souza

Description : Pre-processing of text.
The code does stemmin and tokenization of text
There is a function for lemmatization as well, but it is commented out
'''

import csv
import datetime
import re

from sklearn.feature_extraction.text import TfidfVectorizer

import nltk
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.stem.snowball import SnowballStemmer
stemmer = SnowballStemmer("english")

lem = WordNetLemmatizer()

from pymongo import MongoClient


windows_path = "D:\\StarTV\\LDA Model\\"
azure_path = "/mnt/datafolder/Output/"

ngram_uni_file = windows_path+"Output\\ngrams_uni.csv"
ngram_bi_file = windows_path+"Output\\ngrams_bi.csv"

stopWord_file = windows_path + "Model Datasets/Stopwords.csv"
age_input_3 = windows_path + "Output\\age_prediction_3.csv"

def tokenize_and_stem(text):
    # first tokenize by sentence, then by word to ensure that punctuation is caught as it's own token
    tokens = [word for sent in nltk.sent_tokenize(text) for word in nltk.word_tokenize(sent)]
    filtered_tokens = []
    # filter out any tokens not containing letters (e.g., numeric tokens, raw punctuation)
    for token in tokens:
        if re.search('[a-zA-Z]', token):
            filtered_tokens.append(token)
    stems = [stemmer.stem(t) for t in filtered_tokens]
    return stems

def main():
        
    start_time = datetime.datetime.now()
    print "Start time : ", start_time
    
    ### Reading Stopwords file
    stopWords = []
    with open(stopWord_file, 'rb') as f:
        reader = csv.reader(f)
        for row in reader:
            stopWords.append(row[0].lower())
                        
    print "Reading stopwords file : ", datetime.datetime.now() - start_time
    
    ### Reading Predicted Users file
    Predicted_users = []
    with open(age_input_3, 'rb') as f:
        reader = csv.reader(f)
        for row in reader:
            if row[6] != "" :
                Predicted_users.append(row[0])
    
    print "Reading predicted users file : ", datetime.datetime.now() - start_time
    # Start connection
    
    client = MongoClient()
    db = client.twitter
    collection = db.star
    
    pattern = re.compile('[\W_]+')
    
    month = 4
    query = collection.find({"month" : month}).limit(10000)
    
    rowcount = 0
    tweets = {}
    for docs in query :
        rowcount = rowcount + 1
        print rowcount
        try :
            user = docs['actor']['preferredUsername'].encode('ascii', 'ignore')
            doc_id = docs['_id']

            if user in Predicted_users :
                clusters = docs['topics']
                max = 0
                for cluster in clusters :
                    if max < cluster[1] :
                        max = cluster[1]
                        cluster_no = cluster[0]

                tweet = docs['object']['summary'].encode('ascii', 'ignore')
        
                # Removing punctuations 
                tweet = pattern.sub(' ', tweet)
                
                tweet_text = ""
                for word in tweet.lower().split() :
                    if word not in stopWords :
                        reg_word = re.sub(r'(.)\1+', r'\1\1', word)
                        final_word = lem.lemmatize(reg_word)
                        tweet_text = tweet_text + " " + final_word
                
                if cluster_no not in tweets :
                    tweets[cluster_no] = ""
                tweets[cluster_no] = tweets[cluster_no] + " " + tweet_text.strip()
                
                #id = docs["_id"]
                #collection.update({'_id':id}, {"$set" : {"text": words}}, upsert=False)
        except :
            print "Skipping user"

    client.close()      
    print "Dataset imported to database : ", datetime.datetime.now() - start_time
    
    tfidf_output = windows_path + "Output\\Month\\tfidf_" + str(month) + ".csv"
    writer = csv.writer(open(tfidf_output, 'wb'))
    header = ["Cluster","Word","TFIDF Score"]
    writer.writerow(header)
    
    for cluster_no in tweets :
        print "Cluster : ", cluster_no
        tweet_set = [tweets[cluster_no]]  
    
        #tfidf_vectorizer = TfidfVectorizer(max_features=200000, min_df=0.2, stop_words='english', use_idf=True, tokenizer=tokenize_and_stem, ngram_range=(1,3))

        tf = TfidfVectorizer(analyzer='word', ngram_range=(1,3), min_df = 0.1, stop_words = 'english')
        tfidf_matrix =  tf.fit_transform(tweet_set)
        feature_names = tf.get_feature_names() 
    
        dense = tfidf_matrix.todense().tolist()[0]
    
        phrase_scores = [pair for pair in zip(range(0, len(dense)), dense) if pair[1] > 0]   
        sorted_phrase_scores = sorted(phrase_scores, key=lambda t: t[1] * -1)
        
        rowcount = 0
        for phrase, score in [(feature_names[word_id], score) for (word_id, score) in sorted_phrase_scores]:
            if rowcount < 200 : 
                #print phrase,score
                row = []
                row.append(month)
                row.append(cluster_no)
                row.append(phrase)
                row.append(score)
                writer.writerow(row)

if __name__ == '__main__':
    main()

