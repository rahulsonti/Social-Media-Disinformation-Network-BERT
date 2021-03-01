# -*- coding: utf-8 -*-
"""
Created on Sun Sep 20 18:18:48 2020

@author: rahul
"""
import glob
import sqlite3
import csv


conn = sqlite3.connect(r'C_R_T.db')
cursor = conn.cursor()  

def Turkey_DB(path):    
    cursor.execute('create table if not exists turkey(Screen_Name text, Profile_Description text,Tweet_time date,Tweet_text text, Region text)')    
    allfiles = []
    allfiles = glob.glob(path)
    for f in allfiles:
        with open(f,mode='r',encoding="utf") as my_file:
            list_ = []
            for row in csv.DictReader(my_file):
                 row_dict = {}
                 new_list = []
                 row_dict['Screen_Name']= row['user_screen_name']
                 row_dict['Profile_Description']=row['user_profile_description']
                 row_dict['Tweet_time']= row['tweet_time']                
                 row_dict['Tweet_text']= row['tweet_text']
                 row_dict['Region']= 'Turkey'
                 new_list.append(row_dict['Screen_Name'])
                 new_list.append(row_dict['Profile_Description'])
                 new_list.append(row_dict['Tweet_time'])
                 new_list.append(row_dict['Tweet_text'])
                 new_list.append(row_dict['Region'])
                 list_.append(new_list)
            cursor.executemany("INSERT INTO turkey VALUES(?, ?, ?, ?, ?)", list_)
            conn.commit()



def Russia_DB(path_1):   
    cursor.execute('create table if not exists russia(Screen_Name text, Profile_Description text,Tweet_time date,Tweet_text text, Region text)')
    allfiles = []
    allfiles = glob.glob(path_1)
    for f in allfiles:
        with open(f,mode='r',encoding="utf") as my_file:
            my_reader=csv.DictReader((line.replace('\0','') for line in my_file), delimiter=",")
            list_ = []
            for row in my_reader:
                 row_dict = {}
                 new_list = []
                 row_dict['Screen_Name']= row['user_screen_name']
                 row_dict['Profile_Description']=row['user_profile_description']
                 row_dict['Tweet_time']= row['tweet_time']                
                 row_dict['Tweet_text']= row['tweet_text']
                 row_dict['Region']= 'Russia'
                 new_list.append(row_dict['Screen_Name'])
                 new_list.append(row_dict['Profile_Description'])
                 new_list.append(row_dict['Tweet_time'])
                 new_list.append(row_dict['Tweet_text'])
                 new_list.append(row_dict['Region'])
                 list_.append(new_list)               
            cursor.executemany("INSERT INTO russia VALUES(?, ?, ?, ?, ?)", list_)
            conn.commit()          
  

def China_DB(path_2):
    cursor.execute('create table if not exists china(Screen_Name text, Profile_Description text,Tweet_time date,Tweet_text text, Region text)')
    allfiles = []
    allfiles = glob.glob(path_2)
    for f in allfiles:
        with open(f,mode='r',encoding="utf") as my_file:
            list_ = []
            for row in csv.DictReader(my_file):
                 row_dict = {}
                 new_list = []
                 row_dict['Screen_Name']= row['user_screen_name']
                 row_dict['Profile_Description']=row['user_profile_description']
                 row_dict['Tweet_time']= row['tweet_time']                
                 row_dict['Tweet_text']= row['tweet_text']
                 row_dict['Region']= 'China'
                 new_list.append(row_dict['Screen_Name'])
                 new_list.append(row_dict['Profile_Description'])
                 new_list.append(row_dict['Tweet_time'])
                 new_list.append(row_dict['Tweet_text'])
                 new_list.append(row_dict['Region'])
                 list_.append(new_list)                
            cursor.executemany("INSERT INTO china VALUES(?, ?, ?, ?, ?)", list_)
            conn.commit()


path = "G:/DAEN-690/Turkey/*.csv"     
path_1 = "G:/DAEN-690/Russia/*.csv"  
path_2 = "G:/DAEN-690/China/*.csv"     
  
Turkey_DB(path)
China_DB(path_2)
Russia_DB(path_1)


conn.close() 

