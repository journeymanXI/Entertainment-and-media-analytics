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

csv_path = "D:\\StarTV\\Data\\CSV Files\\"

stopWord_file = "D:\\StarTV\\LDA Model\\Model Datasets\\Stopwords.csv"
punctuations_file = "D:\\StarTV\\LDA Model\\Model Datasets\\Punctuations_2.csv"

age_correlation_13_18 = "D:\\StarTV\\LDA Model\\Model Datasets\\Age Word Correlation\\13_18_bio.csv"
age_correlation_19_25 = "D:\\StarTV\\LDA Model\\Model Datasets\\Age Word Correlation\\19_25_bio.csv"
age_correlation_26 = "D:\\StarTV\\LDA Model\\Model Datasets\\Age Word Correlation\\26_+_bio.csv"

age_output_3 = "D:\\StarTV\\LDA Model\\Output\\age_prediction_3.csv"

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


def read_csv_to_dict1(stopWords,tweet,user,name,user_dictionary):
                         
    ### Extract tweet content and user name
    pattern = re.compile('[\W_]+')
    try :
        tweet_content = tweet.encode('ascii', 'ignore')
        tweet_content = pattern.sub(' ', tweet_content)
        tweet_text = ""
        for word in tweet_content.lower().split() :
            if word not in stopWords :
                tweet_text = tweet_text + " " + re.sub(r'(.)\1+', r'\1\1', word)
        
        if "tweet" not in user_dictionary[user] :
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
def read_csv_to_dict(stopWords,bio,user,name,bot,user_dictionary):
                         
    ### Extract tweet content and user name
    if bot == 0 :
        pattern = re.compile('[\W_]+')
        try :
            content = bio.encode('ascii', 'ignore')
            content = pattern.sub(' ', content)
            content = content.replace("  "," ")
            bio = ""
            for word in content.lower().split() :
                if word not in stopWords :
                    bio = bio + " " + re.sub(r'(.)\1+', r'\1\1', word)
        
            if user not in user_dictionary :
                user_dictionary[user] = {}
                user_dictionary[user]["name"] = name
                user_dictionary[user]["bio"] = bio.strip()
                user_dictionary[user]["words"] = {}
                user_dictionary[user]["bot"] = 0
        
        except :
            print "error at user : ", user
    
    elif bot == 1 :
        if user not in user_dictionary :
            user_dictionary[user] = {} 
            user_dictionary[user]["name"] = name
            user_dictionary[user]["bio"] = bio.strip()
            user_dictionary[user]["words"] = {}
            user_dictionary[user]["bot"] = 1
    
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
 
def main():
    
    start_time = datetime.datetime.now()
    
    print "Starting time : ", start_time
    
    ### Reading Stopwords file
    stopWords = []
    with open(stopWord_file, 'rb') as f:
        reader = csv.reader(f)
        for row in reader:
            stopWords.append(row[0].lower())
            
    print "Reading stopwords file : ", datetime.datetime.now() - start_time
    
    
        ### Reading Stopwords file
    user_dictionary = {}
    with open("D:\\StarTV\\LDA Model\\Output\\age_prediction_4.csv", 'rb') as f:
        reader = csv.reader(f)
        for row in reader:
            if row[0] not in user_dictionary :
                if row[1] == "< 25":
                    user_dictionary[row[0]] = {}
                    user_dictionary[row[0]]["age"] = row[1]

    print "Reading dictionary file : ", datetime.datetime.now() - start_time

    csv_files = [ f for f in listdir(csv_path) if isfile(join(csv_path,f)) ]
    file_count = 0
    for file in csv_files :
        print file
        reader = csv.reader(open(join(csv_path,file), 'r'))
        rc = 0
        for row in reader:
            print rc
            bot = 0
            if rc == 0:
                rc = rc + 1
            else :
                rc = rc + 1
                if row[0] in user_dictionary :
                    user_dictionary = read_csv_to_dict1(stopWords,row[9],row[0],row[1],user_dictionary)
        file_count = file_count + 1
                        
        print "Reading csv file : ", datetime.datetime.now() - start_time     

    user_dictionary = {}
    csv_files = [ f for f in listdir(csv_path) if isfile(join(csv_path,f)) ]
    file_count = 0
    for file in csv_files :
        print file
        reader = csv.reader(open(join(csv_path,file), 'r'))
        rc = 0
        for row in reader:
            bot = 0
            if rc == 0:
                rc += 1
            else :
                if row[2] != "" and row[0] not in user_dictionary :
                #    if float(row[3]) != 0 and float(row[4]) != 0 :
                #        if float(row[3])/float(row[4]) <= 0.02 or float(row[4])/float(row[3]) <= 0.02 :
                #            bot = 1
                #    user_dictionary = read_csv_to_dict(stopWords,row[2],row[0],row[1],bot,user_dictionary)
                    user_dictionary[row[0]] = row[2]
        file_count = file_count + 1
                        
        print "Reading csv file : ", datetime.datetime.now() - start_time
    

    ### Reading Age Lexicon file
    ageCorrelation_dict = {}
    ageCorrelation_dict["13_18"] = ageCorrelation_func(age_correlation_13_18)
    ageCorrelation_dict["19_25"] = ageCorrelation_func(age_correlation_19_25)
    ageCorrelation_dict["26_+"] = ageCorrelation_func(age_correlation_26)
        
    print "Reading Age correlation file : ", datetime.datetime.now() - start_time
        
    rc = 0
    for user in user_dictionary:
        print rc
        rc = rc+1
        if user_dictionary[user]["bot"] == 0 :
            for age_group in ageCorrelation_dict :
                user_score = 0.0
                bio = user_dictionary[user]["bio"]
                counter = Counter(word.lower() for word in re.findall(r"\w+", bio))
                for word in counter :
                    word_count = counter[word]
                    if word in ageCorrelation_dict[age_group][0] :
                        user_score += float(word_count)
                for word in ageCorrelation_dict[age_group][1] :
                    word_count = bio.lower().count(word)
                    user_score += float(word_count)
                user_dictionary[user][age_group] = user_score

    print "Calculating age group : ", datetime.datetime.now() - start_time
        
    writer = csv.writer(open(age_output_3, 'wb'))
    writer.writerow(["user","13-18", "19-25","26_+","bot","age group"])
    for user, sub_dict_1 in user_dictionary.items():
        if user_dictionary[user]["bot"] == 0 :
            row = []
            row.append(user)
            row.append(user_dictionary[user]["13_18"])
            row.append(user_dictionary[user]["19_25"])
            row.append(user_dictionary[user]["26_+"])
            row.append("0")
        
        else :
            row = []
            row.append(user)
            row.append("0")
            row.append("0")
            row.append("0")
            row.append("1")
        
        # Writing max probability group
        if user_dictionary[user]["bot"] == 1 :
            row.append("bot")
        elif user_dictionary[user]["13_18"] == 0 and user_dictionary[user]["19_25"] == 0 and user_dictionary[user]["26_+"] == 0 :
            row.append("")
        elif user_dictionary[user]["13_18"] > user_dictionary[user]["19_25"] and user_dictionary[user]["13_18"] > user_dictionary[user]["26_+"]:
            row.append("13_18")
        elif user_dictionary[user]["19_25"] >= user_dictionary[user]["13_18"] and user_dictionary[user]["19_25"] >= user_dictionary[user]["26_+"]:
            row.append("19_25")
        elif user_dictionary[user]["26_+"] >= user_dictionary[user]["13_18"] and user_dictionary[user]["26_+"] > user_dictionary[user]["19_25"]:
            row.append("26_+")

        writer.writerow(row)
        
    print "Writing to output file : ", datetime.datetime.now() - start_time
        
if __name__ == '__main__':   
    
    main()