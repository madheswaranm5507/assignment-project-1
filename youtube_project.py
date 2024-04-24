from googleapiclient.discovery import build
import streamlit as st
import mysql.connector
import pymongo
import pandas as pd    


# API Key Connection
def Api_connect():

    Api_service_name = "youtube"
    Api_version = "v3"
    Api_id = "AIzaSyCpqyxRbWQPE4dYAq8z5tHux87ZzslAw3o"
    youtube = build(Api_service_name,Api_version,developerKey=Api_id)

    return youtube
youtube = Api_connect()


# MangoDB Connection
client = pymongo.MongoClient("mongodb://localhost:27017/")


# MySQL Connection
mydb = mysql.connector.connect(         
host="localhost",
user="root",
password=""
)
print(mydb)
mycursor = mydb.cursor(buffered=True)



# get Channels information
def get_channel_info(channel_id):
    request = youtube.channels().list(
            part ="snippet,ContentDetails,statistics",
            id = channel_id)
    
    response = request.execute()

    for i in response["items"]:
        data = dict(Channel_Name = i["snippet"]["title"],
                    Channel_Id = i["id"],
                    Subscription_Count = i["statistics"]["subscriberCount"],
                    Channel_views = i["statistics"]["viewCount"],
                    Total_Videos = i["statistics"]["videoCount"],
                    Channel_Description = i["snippet"]["description"],
                    Playlist_Id = i["contentDetails"]["relatedPlaylists"]["uploads"])  
    return data    


# get video ids
def get_video_id(channel_id):
    video_id = []
    response = youtube.channels().list(id =channel_id,
                                    part = "contentDetails").execute()
    Playlist_Id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    next_page_token = None

    while True:
        response1 = youtube.playlistItems().list(
                                                part = "snippet",
                                                playlistId = Playlist_Id,
                                                maxResults = 50,
                                                pageToken = next_page_token).execute()
        for i in range(len(response1["items"])):                            
            video_id.append(response1["items"][i]["snippet"]["resourceId"]["videoId"])
        next_page_token = response1.get("nextPageToken")    

        if next_page_token is None:
            break

    return video_id


# get video information
def get_video_info(video_id):

    video_data = []
    for video_ids in video_id:
        request = youtube.videos().list(
            part = "snippet,ContentDetails,statistics",
            id = video_ids)
        response = request.execute()


        for item in response["items"]:
            data = dict(Channel_Name = item["snippet"]["channelTitle"],
                        Channel_Id = item["snippet"]["channelId"],
                        Video_Id = item["id"],
                        Video_Name = item["snippet"]["title"],
                        Thumbnail = item["snippet"]["thumbnails"]["default"]["url"],
                        Video_Description = item["snippet"].get("description"),
                        Published_Date = item["snippet"]["publishedAt"],
                        Duration = item["contentDetails"]["duration"],
                        View_Count = item["statistics"].get("viewCount"),
                        Like_Count = item["statistics"].get("likeCount"),
                        Comment_Count = item["statistics"].get("commentCount"),
                        Favorite_Count = item["statistics"].get("favoriteCount"),
                        Definition = item["contentDetails"]["definition"],
                        Caption_Status = item["contentDetails"]["caption"]
            )       

            video_data.append(data)

    return video_data


# get comment information
def get_comment_info(video_id):

    comment_data =[]

    try:
        for video_id in video_id:
            request = youtube.commentThreads().list(
                part = "snippet",
                videoId = video_id,
                maxResults = 50
            )

            response = request.execute()

            for detail in response["items"]:
                data = dict(Comment_Id = detail['snippet']['topLevelComment']['id'],
                            Video_id = detail['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_Text= detail['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author= detail['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_PublishedAt= detail['snippet']['topLevelComment']['snippet']['publishedAt'])
            comment_data.append(data)
    except:
        pass
        
    return comment_data



def channel_details(channel_id):
    ch_detail = get_channel_info(channel_id)
    vi_ids  = get_video_id(channel_id)
    vi_detail = get_video_info(vi_ids)
    com_detail = get_comment_info(vi_ids)

    db = client["Youtude_data"] 
    coll1 = db["channel_details"]
    coll1.insert_one({"channel_information" : ch_detail,
                      "video_information" : vi_detail,
                      "comment_information" : com_detail })
    
    return "upload completed successfully"



# data base create
mycursor.execute("create database if not exists project1") 

# channels_table
def channels_table(select_channel_s):
    try:
        create_channel_table = ("""create table if not exists project1.Channels(Channel_Name Varchar(100),
                                                                                Channel_Id Varchar(100) primary key,
                                                                                Subscription_Count Varchar(80),
                                                                                Channel_views Varchar(100),
                                                                                Total_Videos Varchar(100),
                                                                                Channel_Description Text,
                                                                                Playlist_Id Varchar(80))""")
        mycursor.execute(create_channel_table)
        mydb.commit

    except:
        print("channels table already created")


    single_channel_details = []  
    db = client["Youtude_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({"channel_information.Channel_Name":select_channel_s},{"_id" : 0}):
        single_channel_details.append(ch_data["channel_information"])
    df_sigle_channel_details = pd.DataFrame(single_channel_details)  
   


    for index,row in df_sigle_channel_details.iterrows():
        insert_table = ("""INSERT INTO project1.Channels (Channel_Name, Channel_Id, Subscription_Count, Channel_views ,Total_Videos , Channel_Description ,Playlist_Id ) Values (%s,%s,%s,%s,%s,%s,%s)""" )

        values = (row["Channel_Name"],
                row["Channel_Id"],
                row["Subscription_Count"],
                row["Channel_views"],
                row["Total_Videos"],
                row["Channel_Description"],
                row["Playlist_Id"])
        
       
    try:
        mycursor.execute(insert_table,values)
        mydb.commit()
    except:
        news = f"Your Provided Channel Name {select_channel_s} is Already Exists"  

        return news  
        
                                                    


# video_table
def videos_table(select_channel_s):
    create_videos_table = ("""create table if not exists project1.Videos (Channel_Name Varchar(100),
                                                                        Channel_Id Varchar(100),
                                                                        Video_Id  Varchar(100) primary key,
                                                                        Video_Name Varchar(250),
                                                                        Thumbnail Text,
                                                                        Video_Description Text,
                                                                        Published_Date varchar(50),
                                                                        Duration varchar(50),
                                                                        View_Count Int,             
                                                                        Like_Count Int,
                                                                        Comment_Count Int,
                                                                        Favorite_Count Int,
                                                                        Definition Varchar(50), 
                                                                        Caption_Status Varchar(50))""")

    mycursor.execute(create_videos_table)
    mydb.commit()

    single_videos_details = []  
    db = client["Youtude_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({"channel_information.Channel_Name":select_channel_s},{"_id" : 0}):
        single_videos_details.append(ch_data["video_information"])
    df_single_videos_details = pd.DataFrame(single_videos_details[0])     


    for index,row in df_single_videos_details.iterrows():
        insert_videos = ( """INSERT INTO project1.Videos (Channel_Name , Channel_Id, Video_Id, Video_Name, Thumbnail, Video_Description, Published_Date, Duration, View_Count, Like_Count, Comment_Count,Favorite_Count,Definition,Caption_Status) Values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""")

        values = (row["Channel_Name"],
                    row["Channel_Id"],
                    row["Video_Id"],
                    row["Video_Name"],
                    row["Thumbnail"],
                    row["Video_Description"],
                    row["Published_Date"],
                    row["Duration"],
                    row["View_Count"],
                    row["Like_Count"],
                    row["Comment_Count"],
                    row["Favorite_Count"],
                    row["Definition"],
                    row["Caption_Status"])
        
        mycursor.execute(insert_videos,values)
        mydb.commit()



#comment_table
def comments_table(select_channel_s):
    create_comment_table = ("""create table if not exists project1.Comments(Comment_Id Varchar(200) primary key,
                                                                                Video_id Varchar(200),
                                                                                Comment_Text Text,
                                                                                Comment_Author Varchar(200),
                                                                                Comment_PublishedAt varchar(50))""")          
    mycursor.execute(create_comment_table)
    mydb.commit()


    single_comments_details = []  
    db = client["Youtude_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({"channel_information.Channel_Name":select_channel_s},{"_id" : 0}):
        single_comments_details.append(ch_data["comment_information"])
    df_single_comments_details = pd.DataFrame(single_comments_details[0])     



    for index,row in df_single_comments_details.iterrows():
        insert_comment = ("""INSERT INTO project1.Comments(Comment_Id, Video_id, Comment_Text, Comment_Author, Comment_PublishedAt) Values (%s,%s,%s,%s,%s)""")

        values =   (row["Comment_Id"],
                    row["Video_id"],
                    row["Comment_Text"],
                    row["Comment_Author"],
                    row["Comment_PublishedAt"])              
        mycursor.execute(insert_comment,values)
        mydb.commit()



def tables(select_channel):

    news = channels_table(select_channel)
    if news:
        return news
    
    else:
        videos_table(select_channel)
        comments_table(select_channel)

    return "Tables Created Successfully"



def show_channels_table(): 
    channal_list =[]   
    db = client["Youtude_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({},{"_id" : 0, "channel_information" : 1}):
        channal_list.append(ch_data["channel_information"])
    df = st.dataframe(channal_list)

    return df


def show_videos_table():
    video_list =[]
    db = client["Youtude_data"]
    coll1 = db["channel_details"]
    for vi_data in coll1.find({},{"_id" : 0, "video_information" : 1}):
        for i in range(len(vi_data["video_information"])):
            video_list.append(vi_data["video_information"][i])
    df1 = st.dataframe(video_list)

    return df1


def show_comments_table():
    comment_list =[]
    db = client["Youtude_data"]
    coll1 = db["channel_details"]
    for com_data in coll1.find({},{"_id" : 0, "comment_information" : 1}):
        for i in range(len(com_data["comment_information"])):
            comment_list.append(com_data["comment_information"][i])
    df2 = st.dataframe(comment_list)

    return df2



# streamlit data
with st.sidebar:
    st.header("YouTube Data Details Extracted Method ")
    channel_id = st.text_input("Enter the channel ID")

st.title(":green[YOUTUBE DATA HARVESTING AND WAREHOUSING]")

st.header("Introduction")
st.write(" YouTube Data API that allows users to retrieve and analyze data from YouTube channels.It utilizes the YouTube Data API to fetch information such as channel details, video details, comments details and more. The YouTube Data API Reference provides various functionalities to extract and process YouTube data for further analysis and insights.")

st.header("Technologies Used")
st.write("1) Python - The project is implemented using the Python programming language.")
st.write("2) MongoDB - The collected data can be stored in a MongoDB database for efficient data management.")
st.write("3) Pandas - A Powerfull data manipulation in pandas. providing functionalities such as data filtering, dataframe create, transformation, and aggregation.")
st.write("4) MySQL - A Powerfull Open Source Relational database management system used to store and manage the retrieved data.")
st.write("5) Streamlit - The user interface and visualization are created using the Streamlit framework.")

with st.sidebar:
    if st.button("collect and store data in MongoDB"):
        channel_ids = []
        db = client["Youtube_data"]
        coll1 = db["channel_details"]
        for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
            channel_ids.append(ch_data["channel_information"]["Channel_Id"])


all_channels = []
db = client["Youtude_data"]
coll1 = db["channel_details"]
for ch_data in coll1.find({},{"_id" : 0, "channel_information" : 1}):
    all_channels.append(ch_data["channel_information"]["Channel_Name"])

with st.sidebar:
    unique_channel = st.selectbox("Select the channel name",all_channels)

with st.sidebar:
    if st.button("Transfer to MySQL"):
        Table = tables(unique_channel)
        st.success(Table)
with st.sidebar:
    show_table = st.radio("Select the table name",("Channels","Videos","Comments"))

st.header("Tables Details")
if show_table == "Channels":
    show_channels_table()

elif show_table == "Videos":
    show_videos_table()

elif show_table == "Comments":
    show_comments_table()  


st.header("10 Questions and Answer Details")
question = st.selectbox("Select your question"  ,("1. What are the names of all the videos and their corresponding channels",
                                                "2. Which channels have the most number of videos and how many videos do they have",
                                                "3. What are the top 10 most viewed videos and their respective channels",
                                                "4. How many comments were made on each video and what are their corresponding video names",
                                                "5. Which videos have the highest number of likes and what are their corresponding channel names",
                                                "6. What is the total number of likes for each video and what are their corresponding video names",
                                                "7. What is the total number of views for each channel and what are their corresponding channel names",
                                                "8. What are the names of all the channels that have published videos in the year 2022",
                                                "9. What is the average duration of all videos in each channel and what are their corresponding channel names",
                                                "10. Which videos have the highest number of comments and what are their corresponding channel names"))

 
if question == "1. What are the names of all the videos and their corresponding channels":
    query1 = """select Video_Name as videos, channel_name as channelname from project1.Videos"""
    mycursor.execute(query1)
    mydb.commit()
    t1 = mycursor.fetchall()
    df1 = pd.DataFrame(t1,columns = ["video Name","channel name"])
    st.write(df1)

elif question == "2. Which channels have the most number of videos and how many videos do they have":
    query2 = """select channel_name as channelname,total_videos as totalvideos from project1.channels order by total_videos desc"""  
    mycursor.execute(query2)
    mydb.commit()
    t2 = mycursor.fetchall()
    df2 = pd.DataFrame(t2,columns = ["channel name","total videos"])
    st.write(df2)

elif question == "3. What are the top 10 most viewed videos and their respective channels":
    query3 = """select View_Count as views, channel_name as channelname , Video_Name as Videotitle from project1.videos 
                where View_Count is not null order by View_Count desc limit 10"""  
    mycursor.execute(query3)
    mydb.commit()
    t3 = mycursor.fetchall()
    df3 = pd.DataFrame(t3,columns = ["Views","Channel Name","Videotitle"])
    st.write(df3)

elif question == "4. How many comments were made on each video and what are their corresponding video names":
    query4 = """select comment_count as nos_comments, video_name as videotitle from project1.videos where comment_count is not null """  
    mycursor.execute(query4)
    mydb.commit()
    t4 = mycursor.fetchall()
    df4 = pd.DataFrame(t4,columns = ["No Of Comments","Videotitle"])
    st.write(df4)


elif question == "5. Which videos have the highest number of likes and what are their corresponding channel names":
    query5 = """select video_name as videotitle , channel_name as channelname , like_count as likecount
                from project1.videos where like_count is not null order by like_count desc"""  
    mycursor.execute(query5)
    mydb.commit()
    t5 = mycursor.fetchall()
    df5 = pd.DataFrame(t5,columns = ["Videotitle", "Channelname", "Likecount"])
    st.write(df5)

elif question == "6. What is the total number of likes for each video and what are their corresponding video names":
    query6 = """select like_count as likecount, video_name as videotitle from project1.videos"""  
    mycursor.execute(query6)
    mydb.commit()
    t6 = mycursor.fetchall()
    df6 = pd.DataFrame(t6,columns = ["Likecount","Videotitle"])
    st.write(df6)


elif question == "7. What is the total number of views for each channel and what are their corresponding channel names":
    query7 = """select channel_views as totalviews , channel_name as channelname from project1.channels """  
    mycursor.execute(query7)
    mydb.commit()
    t7 = mycursor.fetchall()
    df7 = pd.DataFrame(t7,columns = ["Totalviews" , "Channel name" ])
    st.write(df7)


elif question == "8. What are the names of all the channels that have published videos in the year 2022":
    query8 = """select video_name as video_title , published_date as video_release , channel_name as channelname from project1.videos
                where extract(year from published_date)=2022"""  
    mycursor.execute(query8)
    mydb.commit()
    t8 = mycursor.fetchall()
    df8 = pd.DataFrame(t8,columns = ["Video title","Published date","Channel name"])
    st.write(df8)
    

elif question == "9. What is the average duration of all videos in each channel and what are their corresponding channel names":
    query9 = """SELECT channel_name as channelname,
  SEC_TO_TIME(AVG(
    SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'T', -1), 'M', 1) * 60 +  -- Extract minutes and convert to seconds
    SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'T', -1), 'M', -1)       -- Extract remaining seconds
  )) AS average_duration
FROM 
  project1.videos group by channel_name"""  
    mycursor.execute(query9)
    mydb.commit()
    t9 = mycursor.fetchall()
    df9 = pd.DataFrame(t9,columns = ["Channelname", "Averageduration" ])
    # Average duration convert to string
    T9 = []
    for index,row in df9.iterrows():
        channel_title = row["Channelname"]
        avarage_duration = row["Averageduration"]
        avarage_duration_str = str(avarage_duration)
        T9.append(dict(channelname = channel_title, Averageduration = avarage_duration_str ))
    Df9 = pd.DataFrame(T9)
    st.write(Df9)  


elif question == "10. Which videos have the highest number of comments and what are their corresponding channel names":
    query10 = """select video_name as videotitle , channel_name as channelname , comment_count as comments 
                from project1.videos where comment_count is not null order by comment_count desc"""  
    mycursor.execute(query10)
    mydb.commit()
    t10 = mycursor.fetchall()
    df10 = pd.DataFrame(t10,columns = ["Video title" , "Channel name", "Comments"])
    st.write(df10)


