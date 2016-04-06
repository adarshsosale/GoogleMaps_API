
# coding: utf-8

# In[ ]:

"""RESTful API Requests to Google Maps for Commute Time and Distance Between Locations"""
'''For Inactive Employees only'''

import requests
import pandas as pd
import json
from pandas.io.json import json_normalize
import time
import re

#import dataset of addresses
path1 = 'C:\\Users\\nickb\\Documents\\DataMining_RGA\\Google Maps API\\USA_employee_addresses.xlsx'
df = pd.read_excel(path1)

#filter to only show USA employees (Portland, San Francisco, Los Angeles, Austin, New York)
df = df[df.Country == 'USA']

#prep addresses to be passed to API
df.loc[:,'Home_Address'] = df.Home_Address.str.replace(pat=r'\#\w+', repl='')  #remove apt (e.g. #E, #2B)
df.loc[:,'Home_Address'] = df.Home_Address.str.replace(pat=r'(?i)(apt)\w+', repl='')  #remove apt (e.g. aptE, Apt2B)
df.loc[:,'Home_Address'] = df.Home_Address.str.replace(pat=r'(?i)(apt )\w+', repl='')  #remove apt (e.g. apt E, Apt 2B)
df.loc[:,'Home_Address'] = df.Home_Address.str.replace(pat=r'(?i)(apt)', repl='')  #remove the word 'apt'

df.loc[:,'Home_Address'] = df.Home_Address.str.replace(pat=r'(?i)(unit)\w+', repl='')  #remove unit (e.g. unitE, Unit2B)
df.loc[:,'Home_Address'] = df.Home_Address.str.replace(pat=r'(?i)(unit )\w+', repl='')  #remove unit (e.g. unit E, Unit 2B)
df.loc[:,'Home_Address'] = df.Home_Address.str.replace(pat=r'(?i)(unit)', repl='')  #remove the word 'apt'

df.loc[:,'Home_Address'] = df.Home_Address.str.replace(pat='   ', repl=' ')  #remove excess space (three spaces)
df.loc[:,'Home_Address'] = df.Home_Address.str.replace(pat='  ', repl=' ')  #remove excess space (two spaces)
df.loc[:,'Home_Address'] = df.Home_Address.str.replace(pat=',', repl='')  #remove commas (",")

df.loc[:,'Home_Address'] = df.Home_Address.str.replace(pat='.', repl='')  #remove period from "St."
df.loc[:,'Home_Address'] = df.Home_Address.str.replace(pat=' ', repl='+') #replace space with "+" (req'd for API request)
df.loc[:,'Work_Address'] = df.Work_Address.str.replace(pat=' ', repl='+') #replace space with "+" (req'd for API request)

#segment dataframe by office location
stack = {'TX' : df[df.Work_Sitting == 'Austin'],
         'CHI': df[df.Work_Sitting == 'Chicago'],
         'LA': df[df.Work_Sitting == 'Los Angeles'],
         'NY': df[df.Work_Sitting == 'New York'],
         'OFF': df[df.Work_Sitting == 'Offsite'],
         'PTL': df[df.Work_Sitting == 'Portland'],
         'SF': df[df.Work_Sitting == 'San Francisco']}


# In[ ]:

#load 35 origin/destination addresses for API (35 = approx. max due to HTML character limit of 2,000)

step = 35  #number of addresses to send to API per request

#lists to capture distance and duration data
duration_sec = []
distance_mi = []

#base URL and auth key
url = 'https://maps.googleapis.com/maps/api/distancematrix/json?units=imperial'
auth = 'AIzaSyC7xEPuztHqfDvF7xZKRPOvS5u6SVf73bo'

start = time.time()  #to clock runtime of script

#iterate over sub dataframes in dict
locs = ['TX','CHI','LA','NY','OFF','PTL','SF']
for i in xrange(0,len(locs)):
    office = locs[i]
    begin = min(stack[office].index.values)  #index start for office
    for i in xrange(begin, (begin + stack[office].shape[0]), step):
        home=[]
        h_adrs = stack[office].loc[i:i+(step-1),'Home_Address'].values
        work = stack[office].loc[i,'Work_Address']
        home = h_adrs.tolist()
        for j in xrange(0, len(home)-1):
            home[j] = home[j] + '|'  #add pipe ("|") for multiple addresses
        home_payload = ''.join(home)
        
        #assemble part of request with dict
        payload = {'origins': home_payload,
                   'destinations': work,
                   'key': auth}

        #print 
        print office, work
        print home_payload
        print 'len of list (duration): ', len(duration_sec)
        print ""
        
        #make request to Google Maps API
        response = requests.get(url, params=payload)

        data = response.json()

        #extract relevant distance and duration data from JSON
        pre = json_normalize(data['rows'], 'elements')
        dist = [item['value'] for item in pre.distance]
        dur = [item['value'] for item in pre.duration]
    
        #append duration and distance to running list (to be added back into master dataframe)
        duration_sec = duration_sec + dur
        distance_mi = distance_mi + dist
        
        time.sleep(10.1)  #pause script to avoid hitting throttle limit (100 element requests per 10 seconds)


end = time.time()
runtime = end-start

print "Run Time: %.3f secs" % runtime
print "Run Time: %.3f mins" % (runtime / 60.0)
print "Run Time: %.3f hrs" % (runtime / 3600.0)

