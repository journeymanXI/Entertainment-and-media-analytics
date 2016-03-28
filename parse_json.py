'''
Created on 30-Oct-2015

@author: Jim D'Souza

Description : Code to read a json file, parse it, and store it in another json file in a readable format
'''

import datetime
import json
import csv

filepath_json = "D:\\StarTV\\Data\\JSON Files\\new\\"

def main ():
    
    start_time = datetime.datetime.now()
    print "Starting time : ", start_time
    
    mypath = "D:\\StarTV\\Data\\JSON Files\\new\\"
    
    #mypath = "D:\\StarTV\\Data\\JSON Files\\completed\\"
    from os import listdir
    from os.path import isfile, join
    """
    json_files = [ f for f in listdir(mypath) if isfile(join(mypath,f)) ]
    
    
    rc = 1
    i = 1
    file_json = filepath_json + "overall_" + str(i) + ".json"
    out_file = open(file_json, 'ab')
    
    for file in json_files :
        print rc
        rc += 1
        if rc % 2000 == 0 :
            out_file.close()
            i = i + 1
            file_json = filepath_json + "overall_" + str(i) + ".json"
            out_file = open(file_json, 'ab')

        ### Reading json file
        in_file = open(join(mypath,file), 'r')
        
        line = in_file.readline()
        while line:
            txt = line.strip()
            if txt != "" and not txt.startswith('{"info"'):
                out_file.write(txt)
                out_file.write('\n')
            line = in_file.readline()
        in_file.close()
        
        
    print "Reading json files : ", datetime.datetime.now() - start_time
    """
    
    json_files = [ f for f in listdir(mypath) if isfile(join(mypath,f)) ]
    for file in json_files :
        print file
        in_file = open(join(mypath,file), 'r')
        line = in_file.readline()
        tweets = []
        while line:
            txt = line.strip()
            tweets.append(txt)
            line = in_file.readline()
        in_file.close()
        
        tweet_output = join(mypath,file.strip(".json")+".csv")
    
        writer = csv.writer(open(tweet_output, 'wb'))
        writer.writerow(["User ID","Name","Summary", "Friends","Followers","Lists","Statuses","Location","Device","Tweet","Time"])    
        
        rc = 0
        for tweet in tweets :
            rc = rc + 1
            if tweet != "" :
                try :
                    data = json.loads(tweet)
                    outputrow = []
            
                    user_id = data['actor']['preferredUsername'].encode('ascii', 'ignore')
                    user_name = data['actor']['displayName'].encode('ascii', 'ignore')

                    try :
                        user_summary = data['actor']['summary'].encode('ascii', 'ignore')
                    except : 
                        user_summary = ""
                    
                    user_friends = data['actor']['friendsCount']
                    user_followers = data['actor']['followersCount']
                    user_lists = data['actor']['listedCount']
                    user_status = data['actor']['statusesCount']
                
                    if 'location' in data['actor'] :
                        user_location = data['actor']['location']['displayName'].encode('ascii', 'ignore')
                    elif 'profileLocations' in data['gnip'] :
                        user_location = data['gnip']['profileLocations'][0]['displayName']
                    elif 'location' in data :
                        user_location = data['location']['displayName']
                    else :
                        user_location = ""
                
                    try :
                        user_device = data['generator']['displayName'].encode('ascii', 'ignore')
                    except :
                        user_device = ""

                    tweet_content = data['object']['summary'].encode('ascii', 'ignore')
                    
                    if data['postedTime'] != [] :
                        tweet_time = data['postedTime']
                    elif data['object']['postedTime'] != [] :
                        tweet_time = data['object']['postedTime']
                    else :
                        tweet_time = ""
                        
            
                    outputrow.append(user_id)
                    outputrow.append(user_name)
                    outputrow.append(user_summary)
                    outputrow.append(user_friends)
                    outputrow.append(user_followers)
                    outputrow.append(user_lists)
                    outputrow.append(user_status)
                    outputrow.append(user_location)
                    outputrow.append(user_device)
                    outputrow.append(tweet_content)
                    outputrow.append(tweet_time)
            
                    writer.writerow(outputrow)
            
                except :
                    print "error in line ", rc, " in file ", file
    
    
if __name__ == '__main__':
    main()