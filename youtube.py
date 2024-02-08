import pandas as pd
# import seaborn as sns 
# import numpy as np
from googleapiclient.discovery import build
import streamlit as st
import pymongo
import pymysql 
# import mysql.connector 
import time 
import re
import isodate


load_mongodb = st.container()
load_mysql = st.container()
question_answer = st.container()


st.title('Youtube Data Harvesting Project')
st.subheader('Channel Deatails')
get_channels,channel_run = st.columns([15,1])
# getting the channel id as a input from the user by using streamlit 
get_input = get_channels.text_input('Enter your Youtube id  ')
user_input = get_input

# channel_ids = [ 'UCpNUYWW0kiqyh0j5Qy3aU7w', 'UCnz-ZXXER4jOvuED5trXfEA']
channel_ids = [user_input,]

# api_keys = ['AIzaSyCYfsYTMGua8jL8paIoIy5biijwBRYQpE4', 'AIzaSyBjdZqj1eChlnBJhXpTwxyOo5N-Wcusvb8']
api_key = 'AIzaSyCYfsYTMGua8jL8paIoIy5biijwBRYQpE4'
# channel_id = 'UCpNUYWW0kiqyh0j5Qy3aU7w'
youtube = build('youtube','v3',developerKey=api_key)

# Getting channel details like all details
def get_channel_details(youtube,channel_ids):
    all_data = []
    request = youtube.channels().list(
            part = 'snippet,contentDetails,statistics',
            id = ','.join(channel_ids))
    response = request.execute()
    for ci in range(len(response['items'])):
        data = dict(
            channel_id = response['items'][ci]['id'],
            channel_name = response['items'][ci]['snippet']['title'],
            channel_type = response['items'][ci]['kind'],
            channel_views = response['items'][ci]['statistics']['viewCount'],
            channel_description = response['items'][ci]['snippet']['description'],
            channel_subscibers = response['items'][ci]['statistics']['subscriberCount'],
            channel_videos = response['items'][ci]['statistics']['videoCount'],
            playlist_id = response['items'][ci]['contentDetails']['relatedPlaylists']['uploads'] )
        all_data.append(data)
    return all_data
channels_details = get_channel_details(youtube,channel_ids)
channel_data = pd.DataFrame(channels_details)

# Getting playlists for their respective channels
playlist_ids = list(channel_data['playlist_id'])
def get_playlists(youtube,channel_ids):
    all_data = []
    request = youtube.channels().list(
            part = 'snippet,contentDetails,statistics',
            id = ','.join(channel_ids))
    response = request.execute()
    for pi in range(len(response['items'])):
        data = dict(
            playlist_id = response['items'][pi]['contentDetails']['relatedPlaylists']['uploads'],
            channel_id = response['items'][pi]['id']  )
        all_data.append(data)
    return all_data
playlists = get_playlists(youtube,channel_ids)
playlist_data = pd.DataFrame(playlists)


# Getting video ids for their respective playlist
playlist_id = playlist_ids[0]
def get_videos(youtube,playlist_id):
    video_ids = []
    request = youtube.playlistItems().list(
        part = 'snippet,contentDetails',
        playlistId = playlist_id,
        maxResults = 50 )
    response = request.execute()
    for item in response['items']:
        video_ids.append(item['contentDetails']['videoId'])
    next_page_token = response.get('nextPageToken')
    while next_page_token is not None:
        request = youtube.playlistItems().list(
        part = 'snippet,contentDetails',
        playlistId = playlist_id,
        maxResults = 50,
        pageToken = next_page_token )
        response = request.execute()
        for item in response['items']:
            video_ids.append(item['contentDetails']['videoId'])
        next_page_token = response.get('nextPageToken')
    return video_ids
video_ids = get_videos(youtube,playlist_id)


def get_playlists(youtube,channel_ids):
    request = youtube.channels().list(
            part = 'snippet,contentDetails,statistics',
            id = ','.join(channel_ids))
    response = request.execute()
    for ppi in range(len(response['items'])):
            playlist_id = response['items'][ppi]['contentDetails']['relatedPlaylists']['uploads']
    return playlist_id
playlist_id = get_playlists(youtube,channel_ids)

# Getting video details for their respective video ids
def get_video_details(youtube, video_ids):
    all_data = []
    for vi in range(0, len(video_ids), 50):
        request = youtube.videos().list(
            part='snippet,contentDetails,statistics',
            id=','.join(video_ids[vi:vi+50]))
        response = request.execute()
        for video in response['items']:
            data = {
                'video_id': video['id'],
                'playlist_id':playlist_id,
                'video_title': video['snippet']['title'],
                'video_description': video['snippet']['description'],
                'video_tags': video['snippet'].get('tags', []),
                'video_published': video['snippet']['publishedAt'],
                'video_views': video['statistics']['viewCount'],
                'video_likes': video['statistics']['likeCount'],
                'video_favour': video['statistics']['favoriteCount'],
                'video_comments': video['statistics']['commentCount'],
                'video_thumbnail': video['snippet']['thumbnails']['default']['url'],
                'video_duration': video['contentDetails']['duration'],
                'video_status': video['contentDetails'].get('caption', '')
            }
            all_data.append(data)
    return all_data
video_details=get_video_details(youtube,video_ids)
video_data = pd.DataFrame(video_details)

# Getting comments for their respective videos
def comment_in_videos(youtube,video_ids):
    all_comments = []
    for video_id in video_ids:
        request = youtube.commentThreads().list(
            part = 'id,replies,snippet',
            videoId = video_id )
        response = request.execute()
        for comment in response['items']:
            data = {
                'comment_id':comment['snippet']['topLevelComment']['etag'],
                'video_id':comment['snippet']['topLevelComment']['snippet']['videoId'],
                'comment_author':comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                'comment_published':comment['snippet']['topLevelComment']['snippet']['publishedAt'],
                'comment':comment['snippet']['topLevelComment']['snippet']['textDisplay']
            }
            all_comments.append(data)
    return all_comments
comment_details=comment_in_videos(youtube,video_ids)
comment_df = pd.DataFrame(comment_details)

# Loading the channel details into MongoDB
if channels_details is not None:
    if st.button(' Load to mongodb '):
        client = pymongo.MongoClient('mongodb://localhost:27017')
        mongo_db = client['ryoutube']
        channels = mongo_db['channels']
        channels.drop()
        channels = mongo_db['channels']
        channels.insert_many(channels_details)
        playlists_mongo = mongo_db['playlists_mongo']
        playlists_mongo.drop()
        playlists_mongo = mongo_db['playlists_mongo']
        playlists_mongo.insert_many(playlists)
        videos = mongo_db['videos']
        videos.drop()
        videos = mongo_db['videos']
        videos.insert_many(video_details)
        comments = mongo_db['comments']
        comments.drop()
        comments = mongo_db['comments']
        comments.insert_many(comment_details)

        progress_bar = st.progress(0)
        for i in range(100):
            time.sleep(.01)
            progress_bar.progress(i+1)
        progress_bar.empty()
        st.success('Loaded successfully!')


# Migrating channel details from MongoDB to MySQL
if channels_details is not None:
    if st.button(' Migrate to mysql '):
        client = pymongo.MongoClient('mongodb://localhost:27017')
        mongo_db = client['ryoutube']
        db = pymysql.connect( host="localhost", user="root", password="Samy@1007", database="ryoutube" )
        csr = db.cursor()
        csr.execute('drop table comments ')
        csr.execute('drop table videos ')
        csr.execute('drop table playlist ')
        csr.execute('drop table channels ')
        

        csr.execute("create table channels(channel_id varchar(255) primary key, channel_name varchar(255), channel_type varchar(255), channel_view int, channel_description text, channel_subscriber int, channel_videos int )")
        channels = mongo_db['channels']
        for doc in channels.find():
            c_id = doc['channel_id']
            c_name = doc['channel_name']
            c_type = doc['channel_type']
            c_view = int(doc['channel_views'])
            c_description = doc['channel_description']
            c_subscriber = int(doc['channel_subscibers'])
            c_videos = int(doc['channel_videos'])
            csr.execute(f"INSERT INTO channels(channel_id, channel_name, channel_type, channel_view, channel_description, channel_subscriber, channel_videos ) VALUES ('{c_id}', '{c_name}', '{c_type}', {c_view}, '{c_description}', {c_subscriber}, {c_videos})")
        db.commit()

        csr.execute("create table playlist( playlist_id varchar(255) primary key, channel_id varchar(255), foreign key(channel_id) references channels(channel_id) )")
        playlists_mongo = mongo_db['playlists_mongo']
        for doc in playlists_mongo.find():
            p_id = doc['playlist_id']
            c_id = doc['channel_id']
            csr.execute(f"INSERT INTO playlist(playlist_id, channel_id ) VALUES ('{p_id}', '{c_id}')")
        db.commit()

        csr.execute("create table videos( video_id varchar(255) primary key, playlist_id varchar(255), video_name varchar(255), video_description text, published_date datetime, view_count int, like_count int, favour_count int, comment_count int, duration varchar(255), thumbnail varchar(255), caption_status varchar(255), year int, duration_int int, foreign key(playlist_id) references playlist(playlist_id) )")
        videos = mongo_db['videos']
        v_published = [] 
        video_duration = []
        for doc in videos.find():
            v_id = doc['video_id']
            p_id = doc['playlist_id']
            v_name = doc['video_title']
            clean_name = re.sub(r'\W+', ' ', v_name)
            v_description = doc['video_description']
            clean_text = re.sub(r'\W+', ' ', v_description)
            v_published.append(doc['video_published'])
            publish_df = pd.DataFrame({'publish': v_published})
            publish_df['published']=pd.to_datetime(publish_df['publish'])
            for i in publish_df['published']:
                v_published_date = str(i)  
            v_views = doc['video_views']
            v_likes = doc['video_likes']
            v_favour = doc['video_favour']
            v_comments = doc['video_comments']
            v_duration = doc['video_duration']
            v_thumbnail = doc['video_thumbnail']
            v_status = doc['video_status']
            publish_df['year']=publish_df['published'].dt.year
            for j in publish_df['year']:
                v_year = int(j)
            video_duration.append(doc['video_duration'])
            duration_df = pd.DataFrame({'duration': video_duration})
            duration_df['durations']=duration_df['duration'].apply(lambda a : isodate.parse_duration(a))
            duration_df['durations']=duration_df['durations'].astype('timedelta64[s]')
            duration_df['Durations']=duration_df['durations'].astype('int64')
            for k in duration_df['Durations']:
                v_duration_int = k
            csr.execute(f" insert into videos(video_id, playlist_id, video_name, video_description, published_date, view_count, like_count, favour_count, comment_count, duration, thumbnail, caption_status, year, duration_int) values('{v_id}', '{p_id}', '{clean_name}', '{clean_text}', '{v_published_date}', {v_views}, {v_likes}, {v_favour}, '{v_comments}', '{v_duration}', '{v_thumbnail}', '{v_status}', {v_year}, {v_duration_int} )")
        db.commit()

        csr.execute("create table comments( comment_id varchar(255) primary key , video_id varchar(255), comment_author varchar(255), comment_published datetime, comment text, foreign key(video_id) references videos(video_id) )")
        comments = mongo_db['comments']
        c_published = [] 
        for doc in comments.find():
            c_id = doc['comment_id']
            v_id = doc['video_id']
            c_author = doc['comment_author']
            comment_author = re.sub(r'\W+', ' ', c_author)
            c_published.append(doc['comment_published'])
            comment_df = pd.DataFrame({'comment_published': c_published})
            comment_df['c_published']=pd.to_datetime(comment_df['comment_published'])
            for i in comment_df['c_published']:
                c_published_date = str(i)
            c_text = doc['comment']
            clean_text = re.sub(r'\W+', ' ', c_text)
            csr.execute(f"insert into comments(comment_id, video_id, comment_author, comment_published, comment) values('{c_id}', '{v_id}', '{comment_author}', '{c_published_date}', '{clean_text}' )")
        db.commit()
        # db.close()

        progress_bar = st.progress(0)
        for i in range(100):
            time.sleep(.01)
            progress_bar.progress(i+1)
        progress_bar.empty()
        st.success('Migrated successfully!')

        csr.close()
        db.close()

# Fetching datas from MySQL and display them
if channels_details is not None:
    qdb = pymysql.connect(
    host="localhost",
    user="root",
    password="Samy@1007",
    database="ryoutube"
    )

    cursor = qdb.cursor()

    cursor.execute('select c.channel_name, v.video_name, v.video_id from channels c join playlist p on c.channel_id = p.channel_id join videos v on p.playlist_id = v.playlist_id ')
    channel_video_id = cursor.fetchall()

    cursor.execute('select channel_name, channel_id,channel_videos from channels order by channel_view desc limit 1')
    most_viewed_videos = cursor.fetchall()

    cursor.execute('select  c.channel_id, v.video_name, v.video_id from channels c join playlist p on c.channel_id = p.channel_id join videos v on p.playlist_id = v.playlist_id  order by v.view_count desc limit 10')
    top10_most_viewed_video = cursor.fetchall()

    cursor.execute('select  v.comment_count, v.video_name from channels c join playlist p on c.channel_id = p.channel_id join videos v on p.playlist_id = v.playlist_id  order by v.comment_count desc')
    comment_counts_on_each_video = cursor.fetchall()

    cursor.execute('select  v.video_id, v.video_name, c.channel_id, v.like_count from channels c join playlist p on c.channel_id = p.channel_id join videos v on p.playlist_id = v.playlist_id  order by v.like_count desc limit 1')
    highest_liked_video = cursor.fetchall()

    cursor.execute('select  like_count, view_count, video_name  from videos order by like_count desc, view_count desc')
    most_liked_viewed_videos = cursor.fetchall()

    cursor.execute('select channel_name, channel_view from channels order by channel_view desc')
    channel_views = cursor.fetchall()

    cursor.execute('select v.year, v.video_name from channels c join playlist p on c.channel_id = p.channel_id join videos v on p.playlist_id = v.playlist_id  where v.year = 2022')
    video_published_at_2022 = cursor.fetchall()

    cursor.execute('select distinct c.channel_name, round(avg(v.duration_int),0) as average_watch_hours from channels c join playlist p on c.channel_id = p.channel_id join videos v on p.playlist_id = v.playlist_id  group by c.channel_name')
    channel_average_watch_hours = cursor.fetchall()

    cursor.execute('select v.video_name, v.comment_count, c.channel_name from channels c join playlist p on c.channel_id = p.channel_id join videos v on p.playlist_id = v.playlist_id  order by v.comment_count desc limit 1')
    hightest_comments_video_name = cursor.fetchall()


    #     # Define a dictionary of questions and answers
    dict = {
        " ":"",
        "What are the names of all the videos and their corresponding channels? ": channel_video_id,
        "Which channels have the most number of videos, and how many videos do they have? ": most_viewed_videos,
        "What are the top 10 most viewed videos and their respective channels": top10_most_viewed_video,
        "How many comments were made on each video, and what are theircorresponding video names? ": comment_counts_on_each_video,
        "Which videos have the highest number of likes, and what are their corresponding channel names?": highest_liked_video,
        "What is the total number of likes and dislikes for each video, and what aretheir corresponding video names?": most_liked_viewed_videos,
        "What is the total number of views for each channel, and what are their corresponding channel names?":channel_views,
        "What are the names of all the channels that have published videos in the year 2022?":video_published_at_2022,
        "What is the average duration of all videos in each channel, and what are their corresponding channel names?":channel_average_watch_hours,
        "Which videos have the highest number of comments, and what are their corresponding channel names?":hightest_comments_video_name
    }

    # Create a dropdown list of questions
    selected_question = st.selectbox("Select a question", list(dict.keys()))

    # Display the selected question
    st.write("Selected Question :  ", selected_question)

    # Display the corresponding answer based on the selected question
    if selected_question in dict:
        selected_answer = list(dict[selected_question])
        answer = pd.DataFrame(selected_answer)
        st.write("Answer:", answer)
    
    cursor.close()
    qdb.close()
    