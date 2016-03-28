'''
Created on 28-Oct-2015

@author: Jim D'Souza

Description : Calculating the age group that a twitter user belongs to.
Twitter does not disclose the age of a user.
As a result, we parse the profile if the user has voluntarily disclosed the age.
If not, we use an age calculation algorithm to put users in various age buckets.
This is done by taking into account correlations of certain groups of words with certain age groups.
These words may come from the profile descriptions, or the actual tweets themselves.
'''

import datetime
import json
import csv
import re
from collections import Counter

from os import listdir
from os.path import isfile, join

tweets_file = "D:\\StarTV\\Data\\JSON Files\\overall.json"
csv_path = "D:\\StarTV\\Data\\CSV Files\\"

stopWord_file = "D:\\StarTV\\LDA Model\\Model Datasets\\Stopwords.csv"
punctuations_file = "D:\\StarTV\\LDA Model\\Model Datasets\\Punctuations_2.csv"

age_lexica = "D:\\StarTV\\LDA Model\\Model Datasets\\AgeGenderLexica\\emnlp14age.csv"
age_correlation_13_18 = "D:\\StarTV\\LDA Model\\Model Datasets\\Age Word Correlation\\13_18.csv"
age_correlation_19_22 = "D:\\StarTV\\LDA Model\\Model Datasets\\Age Word Correlation\\19_22.csv"
age_correlation_23_29 = "D:\\StarTV\\LDA Model\\Model Datasets\\Age Word Correlation\\23_29.csv"
age_correlation_30 = "D:\\StarTV\\LDA Model\\Model Datasets\\Age Word Correlation\\30_+.csv"

age_output_1 = "D:\\StarTV\\LDA Model\\Output\\age_prediction_1.csv"
age_output_2 = "D:\\StarTV\\LDA Model\\Output\\age_prediction_2.csv"
age_input_3 = "D:\\StarTV\\LDA Model\\Output\\age_prediction_3.csv"

def read_json_to_dict(stopWords,tweet,user_dictionary):
                         
    ### Extract tweet content and user name
    pattern = re.compile('[\W_]+')
    data = json.loads(tweet)
    try :
        tweet_content = data['text'].encode('ascii', 'ignore')
        user = data['user']['screen_name']
        name = data['user']['name']
        
        tweet_content = pattern.sub(' ', tweet_content)
        tweet_text = ""
        for word in tweet_content.lower().split() :
            if word not in stopWords :
                tweet_text = tweet_text + " " + re.sub(r'(.)\1+', r'\1\1', word)
        
        if user not in user_dictionary :
            user_dictionary[user] = {}
            user_dictionary[user]["name"] = name
            user_dictionary[user]["tweet"] = []
            user_dictionary[user]["tweet"].append(tweet_text.strip())
            user_dictionary[user]["words"] = {}
        
        else :
            user_dictionary[user]["tweet"].append(tweet_content)
        
    except :
        pass
    
    return user_dictionary


def read_csv_to_dict(stopWords,tweet,user,name,user_dictionary):
                         
    ### Extract tweet content and user name
    pattern = re.compile('[\W_]+')
    try :
        tweet_content = tweet.encode('ascii', 'ignore')
        tweet_content = pattern.sub(' ', tweet_content)
        tweet_text = ""
        for word in tweet_content.lower().split() :
            if word not in stopWords :
                tweet_text = tweet_text + " " + re.sub(r'(.)\1+', r'\1\1', word)
        
        if user not in user_dictionary :
            user_dictionary[user] = {}
            user_dictionary[user]["name"] = name
            user_dictionary[user]["tweet"] = []
            user_dictionary[user]["tweet"].append(tweet_text.strip())
            user_dictionary[user]["words"] = {}
        
        else :
            if tweet_text not in user_dictionary[user]["tweet"] :
                user_dictionary[user]["tweet"].append(tweet_text)
        
    except :
        print "error at user : ", user
    
    return user_dictionary

def ageCorrelation_func(file):
    ageCorrelation = {}
    ageCorrelation[0] = {}
    ageCorrelation[1] = {}
    rc = 0
    with open(file, 'rb') as f:
        reader = csv.reader(f)
        for row in reader:
            if rc < 2:
                rc += 1
            else :
                try:
                    if row[2] == '0' :
                        ageCorrelation[0][row[0].lower()] = float(row[1])
                    else :
                        ageCorrelation[1][row[0].lower()] = float(row[1])
                except :
                    pass
    
    return ageCorrelation
   
def findWholeWord(word, string, name=0):
    if name == 0 :
        if re.search(r"\b" + re.escape(word) + r"\b", string) :
            return True
        else :
            return False
        
    elif name == 1 :
        if re.search(word,string) :
            return True
        else :
            return False
 
def main(method, input_file_type):
    
    start_time = datetime.datetime.now()
    
    print "Starting time : ", start_time
    
    ### Reading Stopwords file
    stopWords = []
    with open(stopWord_file, 'rb') as f:
        reader = csv.reader(f)
        for row in reader:
            stopWords.append(row[0].lower())
            
    print "Reading stopwords file : ", datetime.datetime.now() - start_time
    
    Predicted_users = []
    ### Reading Predicted Users file
    stopWords = []
    with open(age_input_3, 'rb') as f:
        reader = csv.reader(f)
        for row in reader:
            if row[5] != "" :
                Predicted_users.append(row[0])
            
    print "Reading Predicted Users file : ", datetime.datetime.now() - start_time
            
    ### Reading json file
    if input_file_type == "json" :
        fp = open(tweets_file, 'r')
        line = fp.readline()
        tweets = []
        while line:
            txt = line.strip()
            tweets.append(txt)
            line = fp.readline()
        fp.close()
        print "Reading json file : ", datetime.datetime.now() - start_time
        
        user_dictionary = {}
        for tweet in tweets :
            user_dictionary = read_json_to_dict(stopWords,tweet,user_dictionary)
    
        print "Moving json file to dictionary : ", datetime.datetime.now() - start_time
        
    elif input_file_type == "csv" :
        user_dictionary = {}
        csv_files = [ f for f in listdir(csv_path) if isfile(join(csv_path,f)) ]
        file_count = 0
        for file in csv_files :
            print file
            file_count = file_count + 1
            if file == "overall_9.csv" :
                reader = csv.reader(open(join(csv_path,file), 'r'))
                rc = 0
                for row in reader:
                    print rc
                    if rc == 0:
                        pass
                    else :
                        if row[0] not in Predicted_users:
                            user_dictionary = read_csv_to_dict(stopWords,row[9],row[0],row[1],user_dictionary)
                    rc = rc + 1
                        
        print "Reading csv file : ", datetime.datetime.now() - start_time
    
    
    
    ##### Method 1
    if method == 1 :
    
        ### Reading Age Lexicon file
        ageLexica = {}
        rc = 0
        with open(age_lexica, 'rb') as f:
            reader = csv.reader(f)
            for row in reader:
                if rc == 0:
                    rc += 1
                else :
                    if row[0].lower() not in ageLexica :
                        ageLexica[row[0].lower()] = {}
                        ageLexica[row[0].lower()]["weight"] = float(row[1])
                        ageLexica[row[0].lower()]["count"] = 0
                    else :
                        ageLexica[row[0].lower()]["weight"] = float(row[1])
            
        print "Reading Age Lexicon file : ", datetime.datetime.now() - start_time
    
        for user in user_dictionary :
            user_word_count = 0
            for tweet in user_dictionary[user]["tweet"] :
                for word in tweet.lower().split() :
                    if word in ageLexica :
                        if word not in user_dictionary[user]["words"] :
                            user_dictionary[user]["words"][word] = 1
                        else :
                            user_dictionary[user]["words"][word] += 1
                        user_word_count += 1
            user_dictionary[user]["word_count"] = user_word_count
    
        print "Counting words in document : ", datetime.datetime.now() - start_time
    
        for user in user_dictionary :
            user_score = 0.0
            for word in user_dictionary[user]["words"] :
                if word in ageLexica :
                    user_score += ageLexica[word]["weight"]*(float(user_dictionary[user]["words"][word])/float(user_dictionary[user]["word_count"]))
            user_score += ageLexica["_intercept"]["weight"]
            user_dictionary[user]["age"] = user_score
    
        print "Calculating ages of users : ", datetime.datetime.now() - start_time
    
        writer = csv.writer(open(age_output_1, 'wb'))
        for key, value in user_dictionary.items():
            writer.writerow([key, value])
        
        print "Writing to output file : ", datetime.datetime.now() - start_time
    
    ##### Method 2
    if method == 2 :
        ### Reading Age Lexicon file
        ageCorrelation_dict = {}
        ageCorrelation_dict["13_18"] = ageCorrelation_func(age_correlation_13_18)
        ageCorrelation_dict["19_22"] = ageCorrelation_func(age_correlation_19_22)
        ageCorrelation_dict["23_29"] = ageCorrelation_func(age_correlation_23_29)
        ageCorrelation_dict["30_+"] = ageCorrelation_func(age_correlation_30)
        
        print "Reading Age correlation file : ", datetime.datetime.now() - start_time
        
        rc = 0
        for user in user_dictionary:
            print rc
            rc = rc+1
            for age_group in ageCorrelation_dict :
                user_score = 0.0
                for tweet in user_dictionary[user]["tweet"] :
                    counter = Counter(word.lower() for word in re.findall(r"\w+", tweet))
                    for word in counter :
                        word_count = counter[word]
                        if word in ageCorrelation_dict[age_group][0] :
                            user_score += float(word_count)*float(ageCorrelation_dict[age_group][0][word])
                    for word in ageCorrelation_dict[age_group][1] :
                        word_count = tweet.lower().count(word)
                        user_score += float(word_count)*float(ageCorrelation_dict[age_group][1][word])
                user_dictionary[user][age_group] = user_score

        print "Calculating age group : ", datetime.datetime.now() - start_time
        
        writer = csv.writer(open(age_output_2, 'wb'))
        writer.writerow(["user","13-18", "19-22","23-29","30+"])
        for user, sub_dict_1 in user_dictionary.items():
            row = []
            row.append(user)
            row.append(user_dictionary[user]["13_18"])
            row.append(user_dictionary[user]["19_22"])
            row.append(user_dictionary[user]["23_29"])
            row.append(user_dictionary[user]["30_+"])
            
            # Writing max probability group
            if user_dictionary[user]["13_18"] > user_dictionary[user]["19_22"] and user_dictionary[user]["13_18"] > user_dictionary[user]["23_29"] and user_dictionary[user]["13_18"] > user_dictionary[user]["30_+"] :
                row.append("13_18")
            elif user_dictionary[user]["19_22"] > user_dictionary[user]["13_18"] and user_dictionary[user]["19_22"] > user_dictionary[user]["23_29"] and user_dictionary[user]["19_22"] > user_dictionary[user]["30_+"] :
                row.append("19_22")
            elif user_dictionary[user]["23_29"] > user_dictionary[user]["13_18"] and user_dictionary[user]["23_29"] > user_dictionary[user]["19_22"] and user_dictionary[user]["23_29"] > user_dictionary[user]["30_+"] :
                row.append("23_29")
            else:
                row.append("30_+")

            writer.writerow(row)
        
        print "Writing to output file : ", datetime.datetime.now() - start_time
        
if __name__ == '__main__':
    method = 2
    input_file_type = "csv"
    main(method,input_file_type)