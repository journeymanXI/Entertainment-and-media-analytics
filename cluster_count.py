'''
Created on 21-Oct-2015

@author: Jim D'Souza

Description : Code to count the number of tweets that belong to each topic.
The topics were created by the LDA algo, and saved with each tweet in MongoDB
'''

import csv
import datetime
import re

from pymongo import MongoClient

windows_path = "D:\\StarTV\\LDA Model\\"

stopWord_file = windows_path + "Model Datasets/Stopwords.csv"
age_input_3 = windows_path + "Output\\age_prediction_3.csv"
cluster_word_file = windows_path + "Output\\Month\\cluster_wordcount.csv"
user_location_file = windows_path + "Output\\Month\\Age_master_file.csv"


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
    Predicted_users = {}
    with open(age_input_3, 'rb') as f:
        reader = csv.reader(f)
        for row in reader:
            user = row[0]
            if row[6] != "" and user not in Predicted_users :
                Predicted_users[user] = {}
    
    ### Reading location file
    
    rc = 0
    genders = ["Male","Female","NA"]
    location_dict = {}
    with open(user_location_file, 'rb') as f:
        reader = csv.reader(f)
        for row in reader:
            if rc == 0 :
                rc = rc + 1
            else :
                user = row[0]
                region = row[6]
                if region != "NA" :
                    city = row[4]
                    state = row[5]
                    if city not in location_dict :
                        location_dict[city] = {}
                        location_dict[city]["state"] = state
                        location_dict[city]["region"] = region
                    if user in Predicted_users :
                        Predicted_users[user]["gender"] = row[7]
                        Predicted_users[user]["location"] = city
    
    print "Reading location file : ", datetime.datetime.now() - start_time
    

    ### Reading words file
    cluster_words = {}
    rc = 0
    with open(cluster_word_file, 'rb') as f:
        reader = csv.reader(f)
        for row in reader:
            if rc == 0 :
                rc = rc + 1
            else :
                month = int(row[0])
                if month not in cluster_words :
                    cluster_words[month] = {}
                    
                cluster_no = int(row[1])
                word = row[2].lower().strip()
                
                if cluster_no not in cluster_words[month] :
                    cluster_words[month][cluster_no] = {}
                    
                #for location in location_dict :
                    #if location not in cluster_words[cluster_no] :
                    #    cluster_words[cluster_no][location] = {}
                    #for gender in genders :
                    #    if gender not in cluster_words[cluster_no][location] :
                    #        cluster_words[cluster_no][location][gender] = {}
                    #    cluster_words[cluster_no][location][gender][word] = {}
                    #    cluster_words[cluster_no][location][gender][word]["tweet count"] = 0
                    #    cluster_words[cluster_no][location][gender][word]["users"] = {}
                segment = row[4]
                if word not in cluster_words[month][cluster_no] :
                    cluster_words[month][cluster_no][word] = {}
                
                cluster_words[month][cluster_no][word]['segment'] = segment
                cluster_words[month][cluster_no][word]['users'] = {}

    
    print "Reading cluster words file : ", datetime.datetime.now() - start_time
    # Start connection
    
    client = MongoClient()
    db = client.twitter
    collection = db.star
    
    pattern = re.compile('[\W_]+')
    
    query = collection.find().limit(100)
    
    rowcount = 0
    pattern = re.compile('[\W_]+')
    
    for docs in query :
        rowcount = rowcount + 1
        print rowcount
        try :
            user = docs['actor']['preferredUsername'].encode('ascii', 'ignore')
            
            if user in Predicted_users :
                #gender = Predicted_users[user]["gender"]
                #location = Predicted_users[user]["location"]
                tweet = docs['object']['summary'].encode('ascii', 'ignore')
                month = docs['month']
                clusters = docs['topics']
                
                max = 0
                cluster_no = 0
                for cluster in clusters :
                    if max < cluster[1] :
                        max = cluster[1]
                        cluster_no = cluster[0]
                
                # Removing punctuations 
                tweet = pattern.sub(' ', tweet)
        
                # Removing stopwords
                tweet_text = ""
                for word in tweet.lower().split() :
                    #if word not in stopWords
                    if word not in stopWords:
                        tweet_text = tweet_text + " " + re.sub(r'(.)\1+', r'\1\1', word)
                """
                for word in cluster_words[cluster_no][location][gender] :
                    if word in tweet_text :
                        cluster_words[cluster_no][location][gender][word]["tweet count"] += 1
                        if user not in cluster_words[cluster_no][location][gender][word]["users"] :
                            cluster_words[cluster_no][location][gender][word]["users"].append(user)
                """
                for word in cluster_words[month][cluster_no] :
                    if word in tweet_text :
                        if user not in cluster_words[month][cluster_no][word]['users'] :
                            cluster_words[month][cluster_no][word]['users'][user] = 0
                        cluster_words[month][cluster_no][word]['users'][user] += 1
        except :
            print "Error"

    client.close()      
    print "Dataset imported from database : ", datetime.datetime.now() - start_time
    
    """
    cluster_tweet_file = windows_path + "Output\\Month\\cluster_location_" + str(month) + ".csv"
    writer = csv.writer(open(cluster_tweet_file, 'wb'))
    header = ["Cluster","Location","State","Region","Gender","Word","Tweet count","User count"]
    writer.writerow(header)
    for cluster_no in cluster_words :
        for location in cluster_words[cluster_no] :
            for gender in cluster_words[cluster_no][location] :
                for word in cluster_words[cluster_no][location][gender] :
                    row = []
                    row.append(cluster_no)
                    row.append(location)
                    row.append(location_dict[location]["state"])
                    row.append(location_dict[location]["region"])
                    row.append(gender)
                    row.append(word)
                    row.append(cluster_words[cluster_no][location][gender][word]["tweet count"])
                    user_count = 0
                    for user in cluster_words[cluster_no][location][gender][word]["users"] :
                        user_count += 1
                    row.append(user_count)
                    writer.writerow(row)
    """
    
    segment_dictionary = {}
    for month in cluster_words :
        
        for cluster_no in cluster_words[month] :
            
            for word in cluster_words[month][cluster_no] :
                segment = cluster_words[month][cluster_no][word]['segment']
                
                if segment not in segment_dictionary :
                    segment_dictionary[segment] = {}

                for user in cluster_words[month][cluster_no][word]['users'] :
                    if user not in segment_dictionary[segment] :
                        segment_dictionary[segment][user] = cluster_words[month][cluster_no][word]['users'][user]
                    
                    
            
    cluster_tweet_file = windows_path + "Output\\Month\\segment_user_count.csv"
    writer = csv.writer(open(cluster_tweet_file, 'wb'))
    header = ["Segment","User","Count"]
    writer.writerow(header)
    for segment in segment_dictionary :
        for user in segment_dictionary[segment] :
            row = []
            row.append(segment)
            row.append(user)
            row.append(segment_dictionary[segment][user])
            writer.writerow(row)   
    print "Cluster location file done : ", datetime.datetime.now() - start_time


if __name__ == '__main__':
    main()
