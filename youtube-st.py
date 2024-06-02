from googleapiclient.discovery import build
import streamlit as st
import pymongo
import mysql.connector
import pandas as pd

api_key= "*****************************"
api_service_name = "youtube"
api_version = "v3"
youtube = build(api_service_name, api_version, developerKey = api_key)

myclient= pymongo.MongoClient("mongodb://localhost:27017/")
mydb_mongo = myclient["Youtube"]
mycol = mydb_mongo["Channel_Data_1"]

mydb_sql= mysql.connector.connect(
            host="localhost",
            user="root",
            password="*****",
            database="youtube_new"
            )

def channel_details(channel_id):
    request = youtube.channels().list(
    part="snippet,contentDetails,statistics",
    id=channel_id)
    response = request.execute()
        
    channel_information = {
        "Channel_Name" : response['items'][0]['snippet']['title'],
        "Channel_Id" : channel_id,
        "Subscription_Count" : response['items'][0]['statistics']['subscriberCount'],
        "Channel_Views" : response['items'][0]['statistics']['viewCount'],
        "Channel_Description" : response['items'][0]['snippet']['description'],
        "Playlist_Id" : response['items'][0]['contentDetails']['relatedPlaylists']['uploads']}
    
    return channel_information

def videoIDs(playlistID):
    video_ids = []
    next_Page_Token = ''
    while True:
        request = youtube.playlistItems().list(
            part="snippet,contentDetails",
            maxResults=25,
            playlistId=playlistID,
            pageToken=next_Page_Token
            )
        response = request.execute()
        for i in range(len(response['items'])):
                if response['items'][i]['contentDetails']['videoId'] is not None:
                    video_id = response['items'][i]['contentDetails']['videoId']
                    video_ids.append(video_id)
                else:
                    video_id = response['items'][i]['snippet']['resourceId']['videoId']
                    video_ids.append(video_id)
        
        next_Page_Token = response.get('nextPageToken')
        if next_Page_Token is not None:
            continue
        else:
            break
    return video_ids

def convert_dur(s):
    l=[]
    f=''
    for i in s:
        if i.isnumeric():
            f=f+i
        else:
            if f:
                l.append(f)
                f=''
    if 'H' not in s:
        l.insert(0,'00')
    if 'M' not in s:
        l.insert(1,'00')
    if 'S' not in s:
        l.insert(2,'00')
    return ':'.join(l)

def videoInformation(video_IDs):
    video_information1=[]
    for i in video_IDs:
        request = youtube.videos().list(
            part='snippet,statistics,contentDetails',id=i)
        response = request.execute()
            
        video_information = {
                             "Video_Id": response['items'][0]['id'],
                             "Video_Name": response['items'][0]['snippet']['title'],
                             "Video_Description": response['items'][0]['snippet']['description'],
                             "Tags" : response['items'][0]['etag'],
                             "PublishedAt" : response['items'][0]['snippet']['publishedAt'],
                             "Channel_Name" : response['items'][0]['snippet']['channelTitle'],
                             "View_Count" : response['items'][0]['statistics']['viewCount'],
                             "Like_Count" : response['items'][0]['statistics'].get('likeCount'),
                             "Comment_Count" : response['items'][0]['statistics'].get('commentCount'),
                             "Duration" : convert_dur(response['items'][0]['contentDetails']['duration']),
                             "Caption_Status" : response['items'][0]['contentDetails']['caption'] }

        video_information1.append(video_information)
        
    return video_information1

def comment_details(video_ids):
    comments = []
    try:
        for i in video_ids:
            request = youtube.commentThreads().list(part='snippet,replies',videoId=i,maxResults=100)
            response = request.execute()
        
            if len(response['items'])>0:
                for j in range(len(response['items'])):
                    comments.append({
                        'Comment_Id': response['items'][j]['snippet']['topLevelComment']['id'],
                        'Video_Id': i,
                        'Comment_Author': response['items'][j]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        'Comment_Text': response['items'][j]['snippet']['topLevelComment']['snippet']['textOriginal'],
                        'Comment_PublishedAt':response['items'][j]['snippet']['topLevelComment']['snippet']['publishedAt'].replace('Z',''),
                        'Comment_Likes': int(response['items'][j]['snippet']['topLevelComment']['snippet']['likeCount'])
                    })
    except:
                comments.append(
            {'Comment_Id':None,'Video_id':i,'Comment_Author':None,'Comment_Text':None,'Comment_PublishedAt':None,'Comment_Likes':None})
    return comments
        
def main(channelID):
    a= channel_details(channelID)
    b= videoIDs(a["Playlist_Id"])
    c= videoInformation(b)
    d= comment_details(b)
    
    channel_data = {"Channel_Data":a,
                    "Video_Data":c,
                    "Comment_Data":d        
    }   
     
    return channel_data

st.title('Youtube Data-Harvesting')
c_id = st.sidebar.text_input("Enter Channel-ID")

values = ['','Extract data','Save data','Query data']
selectbox = st.sidebar.selectbox(
    "Choose an action",values,key='selection')

Q1 = '1. What are the names of all the videos and their corresponding channels?'
Q2 = '2. Which channels have the most number of videos, and how many videos do they have?'
Q3 = '3. What are the top 10 most viewed videos and their respective channels?'
Q4 = '4. How many comments were made on each video, and what are their corresponding video names?'
Q5 = '5. Which videos have the highest number of likes, and what are their corresponding channel names?'
Q6 = '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?'
Q7 = '7. What is the total number of views for each channel, and what are their corresponding channel names?'
Q8 = '8. What are the names of all the channels that have published videos in the year 2022?'
Q9 = '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?'
Q10 = '10. Which videos have the highest number of comments, and what are their corresponding channel names?'

A1 = 'SELECT Video_Name,Channel_Name FROM youtube_new.video;'
A2 = 'select count(Video_Id) as Number_Of_Videos, Channel_Name from youtube_new.video group by Channel_Name order by count(Video_Id) desc limit 10;'
A3 = 'select distinct Video_Name,View_Count,Channel_Name from youtube_new.video order by View_Count desc limit 10;'
A4 = 'select video_name, comment_count from youtube_new.video;'
A5 = 'select distinct video_name,channel_name,Like_Count from youtube_new.video order by Like_Count desc limit 10'
A6 = 'SELECT Like_Count,Video_Name FROM youtube_new.video;'
A7 = 'select Channel_Name, sum(view_count) as Total_Views from youtube_new.video group by Channel_Name'
A8 = "select Channel_Name from youtube_new.video where PublishedAt like '%2022%' group by Channel_Name;"
A9 = 'SELECT SEC_TO_TIME(AVG(TIME_TO_SEC(Duration))) AS Average_Time,Channel_Name FROM youtube_new.video group by Channel_Name;'
A10 = 'select distinct Video_Name,Channel_Name,comment_count from youtube_new.video order by Comment_Count desc limit 5;'

question_list = ['',Q1,Q2,Q3,Q4,Q5,Q6,Q7,Q8,Q9,Q10]

if selectbox is not 'Query data':
    selectbox_query = st.selectbox("Select your question",question_list,disabled=True,key='on_selection')
else:
    selectbox_query = st.selectbox("Select your question",question_list,disabled=False)
    if selectbox_query is Q1:
        mycursor=mydb_sql.cursor()
        mycursor.execute(A1)
        C1 = mycursor.fetchall()
        df1 = pd.DataFrame(C1,columns=['Video_Name','Channel_Name'])
        st.write(df1)
    
    if selectbox_query is Q2:
        mycursor=mydb_sql.cursor()
        mycursor.execute(A2)
        C2 = mycursor.fetchall()
        df2 = pd.DataFrame(C2,columns=['Number_Of_Videos','Channel_Name'])
        st.write(df2)

    if selectbox_query is Q3:
        mycursor=mydb_sql.cursor()
        mycursor.execute(A3)
        C3 = mycursor.fetchall()
        df3 = pd.DataFrame(C3,columns=['Video_Name','View_Count','Channel_Name'])
        st.write(df3)
    
    if selectbox_query is Q4:
        mycursor=mydb_sql.cursor()
        mycursor.execute(A4)
        C4 = mycursor.fetchall()
        df4 = pd.DataFrame(C4,columns=['Video_Name','Comment_Count'])
        st.write(df4)
    
    if selectbox_query is Q5:
        mycursor=mydb_sql.cursor()
        mycursor.execute(A5)
        C5 = mycursor.fetchall()
        df5 = pd.DataFrame(C5,columns=['Video_Name','Channel_Name','Like_Count'])
        st.write(df5)
    
    if selectbox_query is Q6:
        mycursor=mydb_sql.cursor()
        mycursor.execute(A6)
        C6 = mycursor.fetchall()
        df6 = pd.DataFrame(C6,columns=['Like_Count','Video_Name'])
        st.write(df6)
    
    if selectbox_query is Q7:
        mycursor=mydb_sql.cursor()
        mycursor.execute(A7)
        C7 = mycursor.fetchall()
        df7 = pd.DataFrame(C7,columns=['Channel_Name','Total_Views'])
        st.write(df7)
    
    if selectbox_query is Q8:
        mycursor=mydb_sql.cursor()
        mycursor.execute(A8)
        C8 = mycursor.fetchall()
        df8 = pd.DataFrame(C8,columns=['Channel_Name'])
        st.write(df8)
    
    if selectbox_query is Q9:
        mycursor=mydb_sql.cursor()
        mycursor.execute(A9)
        C9 = mycursor.fetchall()
        df9 = pd.DataFrame(C9,columns=['Average_Time','Channel_Name'])
        st.write(df9)
    
    if selectbox_query is Q10:
        mycursor=mydb_sql.cursor()
        mycursor.execute(A10)
        C10 = mycursor.fetchall()
        df10 = pd.DataFrame(C10,columns=['Video_Name','Channel_Name','Comment_Count'])
        st.write(df10)
        
if st.sidebar.button('Submit'):
        
    if selectbox is 'Extract data':
        result = main(c_id)
        st.text(result)
        mycol.insert_one(result)
        st.success("Data inserted in MongoDB")

        
    if selectbox is 'Save data':
        m_data = mycol.find_one({"Channel_Data.Channel_Id":c_id})
        mydb_sql= mysql.connector.connect(
            host="localhost",
            user="root",
            password="*****",
            database="youtube_new"
            )
        mycursor = mydb_sql.cursor()
        mycursor.execute("create database if not exists youtube_new")
        
        mycursor.execute('''create table if not exists channel (channel_name varchar(255),channel_id varchar(255),subscription_count int,channel_views int,
        channel_description varchar(5000),Playlist_Id varchar(255))''')
        mycursor.execute('''create table if not exists comment (Comment_Id varchar(255),Video_Id varchar(255),Comment_Author varchar(255),Comment_Text    
        varchar(6000),Comment_PublishedAt varchar(255),Comment_Likes int)''')
        mycursor.execute('''create table if not exists video (Video_Id varchar(255),Video_Name varchar(5000),Video_Description varchar(5000),Tags varchar(255),
        PublishedAt varchar(255),Channel_Name varchar(255),View_Count int,Like_Count int,Comment_Count int,Duration varchar(50),Caption_Status varchar(255))''')
        
        query1 = '''insert into channel(channel_name,channel_id,subscription_count,channel_views,channel_description,Playlist_Id) values(%s,%s,%s,%s,%s,%s)'''
        query2 = '''insert into comment (Comment_Id,Video_Id,Comment_Author,Comment_Text,Comment_PublishedAt,Comment_Likes) values(%s,%s,%s,%s,%s,%s)'''
        query3 = '''insert into video (Video_Id,Video_Name,Video_Description,Tags,PublishedAt,Channel_Name,View_Count,Like_Count,Comment_Count,Duration,
        Caption_Status) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        
        if m_data is not None:
            value1 = tuple(m_data['Channel_Data'].values())
            mycursor.execute(query1,value1)
            mydb_sql.commit()
            for i in range(len(m_data['Comment_Data'])):
                value2 = tuple(m_data['Comment_Data'][i].values())
                mycursor.execute(query2,value2)
                mydb_sql.commit()
            for i in range(len(m_data['Video_Data'])):
                value3 = tuple(m_data['Video_Data'][i].values())
                mycursor.execute(query3,value3)
                mydb_sql.commit()
            st.success("Data inserted in MySQL")
        else:
            st.success("Data not found in MongoDB")
