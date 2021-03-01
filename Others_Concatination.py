# -*- coding: utf-8 -*-
"""
Created on Thu Sep 24 21:22:01 2020

@author: rahul
"""
import json
import pandas as pd
import os
import bz2
import time
import sqlite3


start = time.time()

conn = sqlite3.connect(r'Otherstraining.db')
cursor = conn.cursor()

def tarfiles_to_table_as_otherstraining(root_path, csv_file_name):
    cursor.execute('create table if not exists otherstraining(Screen_Name text,ID text, Profile_Description text,Tweet_text text, Region text)')
    root_path = root_path.replace("\\", "/")
    i = 0
    for root,_,files in os.walk(root_path):
        json_data = []
        for name in files:
            path = os.path.join(root,name)
            print("Processing file with name: ", path)                      
            with bz2.open(path, "rb") as f: 
                for line in f.readlines():
                    line_dict = {}
                    new_list = []
                    json_dat = json.loads(line.decode('utf-8'))
                    if (pd.notnull(json_dat.get('user'))) & (json_dat.get('delete') == None):
                        line_dict['Screen_Name']=json_dat['user']['screen_name']
                        line_dict['ID']=json_dat['user']['id_str']
                        line_dict['Profile_Description']=json_dat['user']['description']
                        line_dict['Tweet_text']=json_dat['text']
                        line_dict['Region']='Others'
                        new_list.append(line_dict['Screen_Name'])
                        new_list.append(line_dict['ID'])
                        new_list.append(line_dict['Profile_Description'])
                        new_list.append(line_dict['Tweet_text'])
                        new_list.append(line_dict['Region'])
                        json_data.append(new_list)
        cursor.executemany("INSERT INTO otherstraining VALUES(?, ?, ?, ?, ?)", json_data)
        conn.commit()
        print("Data written to file", csv_file_name+str("_")+str(i)+".csv")
#        df = pd.DataFrame(json_data)
#        df.to_csv(csv_file_name+str("_")+str(i)+".csv")
        i += 1

tarfiles_to_table_as_otherstraining(r"G:\DAEN-690\Others-training", "json_data")
end = time.time()
print("Runtime of the program is", {end - start})
conn.close() 






start = time.time()

conn = sqlite3.connect(r'Others-testing.db')
cursor = conn.cursor()

def tarfiles_to_table_as_otherstesting(root_path, csv_file_name):
    cursor.execute('create table if not exists otherstesting(Screen_Name text,ID text, Profile_Description text,Tweet_text text, Region text)')
    root_path = root_path.replace("\\", "/")
    i = 0
    for root,_,files in os.walk(root_path):
        json_data = []
        for name in files:
            path = os.path.join(root,name)
            print("Processing file with name: ", path)                      
            with bz2.open(path, "rb") as f: 
                for line in f.readlines():
                    line_dict = {}
                    new_list = []
                    json_dat = json.loads(line.decode('utf-8'))
                    if (pd.notnull(json_dat.get('user'))) & (json_dat.get('delete') == None):
                        line_dict['Screen_Name']=json_dat['user']['screen_name']
                        line_dict['ID']=json_dat['user']['id_str']
                        line_dict['Profile_Description']=json_dat['user']['description']
                        line_dict['Tweet_text']=json_dat['text']
                        line_dict['Region']='Others'
                        new_list.append(line_dict['Screen_Name'])
                        new_list.append(line_dict['ID'])
                        new_list.append(line_dict['Profile_Description'])
                        new_list.append(line_dict['Tweet_text'])
                        new_list.append(line_dict['Region'])
                        json_data.append(new_list)
        cursor.executemany("INSERT INTO otherstesting VALUES(?, ?, ?, ?, ?)", json_data)
        conn.commit()
        print("Data written to file", csv_file_name+str("_")+str(i)+".csv")
#        df = pd.DataFrame(json_data)
#        df.to_csv(csv_file_name+str("_")+str(i)+".csv")
        i += 1

tarfiles_to_table_as_otherstesting(r"G:\DAEN-690\Others-testing", "json_data")

end = time.time()

print("Runtime of the program is", {end - start})

conn.close() 





# =============================================================================
# SELECT *
# FROM russia 
# WHERE tweet_time BETWEEN '2019-06-01T00:00:00.00' AND '2019-06-30T23:59:59.999'
# 
# CREATE TABLE junerussia (Screen_Name text, Profile_Description text, Tweet_text text, Region text) 
# INSERT INTO junerussia SELECT Screen_Name,Profile_Description,Tweet_text,Region FROM russia WHERE tweet_time BETWEEN '2019-06-01T00:00:00.00' AND '2019-06-30T23:59:59.999';
# 
# ATTACH DATABASE 'G:\DAEN-690\C_R_T.db' AS crt_db
# 
# SELECT DISTINCT(otherstraining.Screen_Name) FROM junerussia INNER JOIN otherstraining on junerussia.Screen_Name=otherstraining.Screen_Name or junerussia.Screen_Name = otherstraining.ID;
# 
# 
# CREATE TABLE juneothersrussia (Screen_Name text,Tweet_text text, Region text)
# 
# INSERT INTO juneothersrussia SELECT * FROM otherstraining WHERE otherstraining.Screen_Name NOT IN (SELECT DISTINCT(otherstraining.Screen_Name) FROM junerussia INNER JOIN otherstraining on junerussia.Screen_Name=otherstraining.Screen_Name or junerussia.Screen_Name = otherstraining.ID);
# =============================================================================





