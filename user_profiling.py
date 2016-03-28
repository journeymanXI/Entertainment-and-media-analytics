'''
Created on 27-Nov-2015

@author: Jim D'Souza

Description : I have discovered topics being spoken about in tweets, and attributed to tweets to them.
After this, I have found out the users who are talking about particular topics.
Now, I cluster users based on the topics they speak about, to create a profile of users.
I test out K-Means clustering and Agglomerative clustering to do this.
'''

import numpy as np
import pandas as pd
import csv

import time
import datetime
from sklearn.cluster import MiniBatchKMeans, AgglomerativeClustering


from matplotlib import pyplot

start_time = datetime.datetime.now()
print "Starting time : ", start_time

user_file = "D:\\StarTV\\LDA Model\\Output\\cluster_location_5.csv"
user_pd = pd.read_csv(user_file).fillna(0)

#user_pd = user_pd.drop('Real Estate', 1)

output_file = "D:\\StarTV\\LDA Model\\Output\\cluster_user_profiles_high.csv"

writer = csv.writer(open(output_file, 'wb'))
header = ["zone","user","cluster no"]
writer.writerow(header)

def cluster_function(user_np):
    ##############################################################################
    # Compute clustering with Means
    if len(user_np) < 10 :
        n_cl = 2
    elif len(user_np) <= 100 :
        n_cl = 10
    elif len(user_np) <= 500 :
        n_cl = 15
    elif len(user_np) <= 1000 :
        n_cl = 20
    else :
        n_cl = 30

    k_means = MiniBatchKMeans(n_clusters=n_cl, init='k-means++', max_iter=100, batch_size=100, verbose=0, compute_labels=True, 
                              random_state=None, tol=0.0, max_no_improvement=10, init_size=None, n_init=3, reassignment_ratio=0.01)

    t0 = time.time()
    k_means.fit(user_np)
    
    t_batch = time.time() - t0
    print "Batch running time : ", t_batch
    
    k_means_labels = k_means.labels_
    
    #prediction = k_means.predict(user_np)
    return k_means_labels
    
"""
# For each region in dataset
regions = pd.unique(user_pd["Region"].ravel())

for region in regions :
    print "Region for clustering : ", region
    region_pd = user_pd[user_pd['Region'] == region]
    
    region_np = region_pd.as_matrix()
        
    # Getting user list
    region_users = region_np[:,0]
    
    # Prepping dataset for clustering
    region_np = region_np[:,5:]
    
    labels = cluster_function(region_np)
    
    for i in range(len(region_users)) :
        row = []
        row.append(region)
        row.append(region_users[i])
        row.append(labels[i])
        writer.writerow(row)
  
    print "Region ", region, " clustering completed in : ",datetime.datetime.now() - start_time


# For each state in dataset
states = pd.unique(user_pd["State"].ravel())

for state in states :
    print "State for clustering : ", state
    state_pd = user_pd[user_pd['State'] == state]
    
    state_np = state_pd.as_matrix()
        
    # Getting user list
    state_users = state_np[:,0]
    
    # Prepping dataset for clustering
    state_np = state_np[:,5:]
    
    labels = cluster_function(state_np)
    
    for i in range(len(state_users)) :
        row = []
        row.append(state)
        row.append(state_users[i])
        row.append(labels[i])
        writer.writerow(row)
  
    print "State ", state, " clustering completed in : ",datetime.datetime.now() - start_time
"""

# Overall 
user_np = user_pd.as_matrix()

headers = list(user_pd.columns.values)

# Getting user list
users = user_np[:,0]
priority = user_np[:,1]
gender = user_np[:,2]

user_dict = {}
for row in user_np :
    user = row[0]
    total = 0
    if row[1] == "High":
        user_dict[user] = {}
        for i in range(len(headers)) :
            if i >= 3 :
                user_dict[user][headers[i]] = int(row[i])
                total += int(row[i])
        user_dict[user]["total count"] = total
        user_dict[user]["gender"] = row[2]
    
# Prepping dataset for clustering
user_np = user_np[:,3:]

labels = cluster_function(user_np)
    
cluster_dict = {}
for i in range(len(user_np)) :
    row = []
    row.append("All India")
    row.append(users[i])
    row.append(labels[i])
    writer.writerow(row)
    
    cluster = labels[i]
    if cluster not in cluster_dict :
        cluster_dict[cluster] = {}
    
    for topic in user_dict[users[i]] :
        if topic not in cluster_dict[cluster] :
            cluster_dict[cluster][topic] = {}
            cluster_dict[cluster][topic]['users'] = {}
            cluster_dict[cluster][topic]['tweets'] = 0
            cluster_dict[cluster][topic]['users']['total'] = 0
            cluster_dict[cluster][topic]['users']['male'] = 0
            cluster_dict[cluster][topic]['users']['female'] = 0
        
        if topic != "total count" and topic != "gender" :
            cluster_dict[cluster][topic]['tweets'] += user_dict[users[i]][topic]
        
            if user_dict[users[i]][topic] > 0 :
                cluster_dict[cluster][topic]['users']['total'] += 1
                if user_dict[users[i]]["gender"] == "Male":
                    cluster_dict[cluster][topic]['users']['male'] += 1
                elif user_dict[users[i]]["gender"] == "Female":
                    cluster_dict[cluster][topic]['users']['female'] += 1
  
print "Overall clustering completed in : ",datetime.datetime.now() - start_time

output_file2 = "D:\\StarTV\\LDA Model\\Output\\cluster_profiles_high.csv"

writer = csv.writer(open(output_file2, 'wb'))
header = ["cluster no","Topic", "Tweet count", "User count", "Male count", "Female count"]
writer.writerow(header)

for cluster in cluster_dict :
    for topic in cluster_dict[cluster] :
        if topic != "total count" and topic != "gender" :
            row = []
            row.append(cluster)
            row.append(topic)
            row.append(cluster_dict[cluster][topic]['tweets'])
            row.append(cluster_dict[cluster][topic]['users']['total'])
            row.append(cluster_dict[cluster][topic]['users']['male'])
            row.append(cluster_dict[cluster][topic]['users']['female'])
            writer.writerow(row)