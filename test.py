from flask import Flask, render_template, request
import time
import os
import glob
import shutil
from flask_cors import cross_origin
from selenium import webdriver
from selenium.common import exceptions
from pytube import YouTube
import boto3
import json
from botocore.exceptions import NoCredentialsError
import mysql.connector as connection
import pandas as pd
import pymongo
client = pymongo.MongoClient("mongodb+srv://ritusahu:ritusahupwd@cluster0.telj5.mongodb.net/?retryWrites=true&w=majority")
def databaseRelated():
    # ("creating database & table")
    try:
        # cursor.execute('CREATE DATABASE IF NOT EXISTS youtubeScraper')
        # # ("creating tables")
        # yt_info = 'create table if not exists youtubeScraper.youtuberscraperdata( youtuber_name varchar(50),video_link varchar(50),downloaded_video_path varchar(50),aws_link varchar(100),likes varchar(20),no_of_comments varchar(20),title varchar(100))'
        # cursor.execute(yt_info)
        cwd = os.getcwd()+"\youtuberInfo.csv"
        yt_df = pd.read_csv(cwd)
        payload_yt = json.loads(yt_df.to_json(orient='records'))
        database = client['youtubeData']
        collection1 = database['youtuberscraperdata']
        # ("Iterating dataframe & inserting comments record in mongodb")
        for data in payload_yt:
            get = collection1.count_documents({'yt_link': data['yt_link']})
            if (get == 0):
                collection1.insert_one(data)
            else:
                pass

        # ("Iterating dataframe & inserting records in mysql")
        # for (rows, rs) in yt_df.iterrows():
        #     # ("Checking for duplicate records")
        #     record = 'select count(*) as count from youtubeScraper.youtuberscraperdata where video_link ="' + str(
        #         rs[1]) + '"'
        #     cursor.execute(record)
        #     rowcount = cursor.fetchone()[0]
        #     if (rowcount > 0):
        #         continue
        #     else:
        #         # qry = "insert into youtubeScraper.youtuberscraperdata values(""'" + str(rs[0]) + "','" + str(
        #         # rs[1]) + "','" + str(rs[2]) + "','" + str(rs[3]) + "','" + str(rs[4]) + "','" + str(
        #         # rs[5]) + "','" + str(rs[6]) + "')"
        #
        #         qry = 'insert into youtubeScraper.youtuberscraperdata values(''"' + str(rs[0]) + '","'+ str(
        #             rs[1]) + '","' + str(rs[2]) + '","' + str(rs[3]) + '","' + str(rs[4]) + '","' + str(
        #             rs[5]) + '","' + str(rs[6]) + '")'
        #         print(qry)
        #         cursor.execute(qry)
        #         mydb.commit()
        # ("data inserted")
        csv_path = os.getcwd() + "\comments.csv"
        # yt_df = pd.read_csv(cwd)
        data1 = pd.read_csv(csv_path)
        payload = json.loads(data1.to_json(orient='records'))
        database = client['youtubeData']
        collection = database['youtuberCommentsDetails']
        # ("Iterating dataframe & inserting comments record in mongodb")
        for data in payload:
            get = collection.count_documents({'yt_link': data['yt_link'], 'commenter_name': data['commenter_name'], 'comments': data['comments']})
            if (get == 0):
                collection.insert_one(data)
            else:
                pass

    except Exception as e:
        print(e)

def getAllDataFromDB():
    # ("getting all youtuber details from database")
    try:
        database = client['youtubeData']
        collection = database['youtuberscraperdata']
        # ("getting all comments from link")
        yt_details = collection.find()
    except Exception as e:
        print(e)
    return yt_details

databaseRelated()

for detail in getAllDataFromDB():
    print(detail)