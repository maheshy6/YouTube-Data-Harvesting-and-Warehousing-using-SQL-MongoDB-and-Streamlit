from googleapiclient.discovery import build
from pymongo import MongoClient
import pandas as pd
import streamlit as st
import mysql.connector

#API KEY connection

def Api_key_connection():
  api_service_name = "youtube"
  api_version = "v3"
  api_key = "AIzaSyATz7kc3grDd7A1h_pSR76M4n7RZb63z7k"

  youtube =build(api_service_name, api_version, developerKey=api_key)
  return youtube
youtube=Api_key_connection()

#Creating function for collecting channel information
def channel_info(channel_id):
  request=youtube.channels().list(
                  id=channel_id,
                  part="snippet,statistics,contentDetails")
  response=request.execute()

  for i in response["items"]:
    channel_informations =dict(channel_name=i["snippet"]["title"],
                              Channel_Id=i["id"],
                              Subscription_Count=i["statistics"]["subscriberCount"],
                              Video_count=i["statistics"]["videoCount"],
                              Channel_Views=i["statistics"]["viewCount"],
                              Channel_Description=i["snippet"]["description"],
                              Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
  return channel_informations

#Creating function for collecting Playlist information
def playlist_ids(channel_id):
    response=youtube.channels().list(
                                    id=channel_id,
                                    part="contentDetails").execute()
    Playlist_Id=response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
        
    Next_page_token=None
    VideoIds=[]
    while True:
        response2=youtube.playlistItems().list(part="snippet",                                      
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=Next_page_token).execute()
        
        for i in response2["items"]:
            video_id=i["snippet"]["resourceId"]["videoId"]
            VideoIds.append(video_id)
        Next_page_token=response2.get("nextPageToken")
        if Next_page_token is None:
            break
    return VideoIds


#Creating function for collecting Video information
def Getting_video_info(Play_listid):
  Video_info=[]
  n=1
  for video_id in Play_listid:
    response3=youtube.videos().list(
                                    part="snippet,contentDetails,statistics",
                                    id=video_id).execute()
    for item in response3["items"]:
      data=dict(channel_name=item["snippet"]["channelTitle"],
                Channel_Id=item["snippet"]["channelId"],
                Video_Id=item["id"],
                Video_Name=item["snippet"]["title"],
                Thumbnail=item["snippet"]["thumbnails"]["default"]["url"],
                Video_Description=item["snippet"].get("description"),
                PublishedAt=item["snippet"]["publishedAt"],
                View_Count=item["statistics"].get("viewCount"),
                Like_Count=item["statistics"]["likeCount"],
                Favorite_Count=item["statistics"]["favoriteCount"],
                Comment_Count=item["statistics"].get("commentCount"),
                Caption_Status=item["contentDetails"]["caption"])
    Video_info.append(data)

  return Video_info

#Creating function for collecting Comment information
def Getting_comment_info(video_ids):
  try:
    comment_ids=[]
    Nextpage=None
    for i in video_ids:
      response4=youtube.commentThreads().list(
                                        part="snippet",
                                        videoId=i,
                                        maxResults=50,
                                        pageToken=Nextpage).execute()
      Nextpage=response4.get("nextPageToken")
      comments = response4["items"]
      for comment in comments:
        data2=dict(Video_id=i,
                  Comment_Id=comment["snippet"]["topLevelComment"]["id"],
                  Comment_Text=comment["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                  Comment_Author=comment["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                  Comment_PublishedAt=comment["snippet"]["topLevelComment"]["snippet"]["publishedAt"])
        comment_ids.append(data2)
      if Nextpage is None:
        break
    return comment_ids
  except:
    pass



#MongoDb Connection


#creating database in MongoDB
client=MongoClient("mongodb+srv://maheshy675:fgPLc4dkJ6h3FXq9@cluster0.vz8zgjz.mongodb.net/")
db=client.StreamlitProject
collect=db.YoutubeData

#Creating function for collecting Playlist details and upload into mongodb
def channel_information(channel_id):
            
    ch_dt=channel_info(channel_id)
    pid_dt=playlist_ids(channel_id)
    vd_dt=Getting_video_info(pid_dt)
    cm_dt=Getting_comment_info(pid_dt)
    
    collect.insert_one({"Channel_information":ch_dt,
                        "Video_information":vd_dt,
                        "Comment_information":cm_dt})
    
    return "Channel details is Upload Successfully in MongoDb"

# sql connection

mydb = mysql.connector.connect(host="localhost",user="root",password="12345",database = "youtube_project")
mycursor = mydb.cursor()


#creating table for channel details in sql database
def Channels_Table(channelnames):
    mycursor.execute("drop table if exists Channel")
    mydb.commit()

    try:
        mycursor.execute('''create table Channel(channel_name varchar(100),
                                            Channel_Id varchar(100) ,
                                            Subscription_Count bigint,
                                            Video_count bigint,
                                            Channel_Views bigint,
                                            Channel_Description text,
                                            Playlist_Id varchar(100))''')
        mydb.commit()
    except:
        print("Table was already created")

    # creat dataframe in pandas
    ls=channelnames
    ch_list=[]
    for i in ls:    
        dt1=collect.find({"Channel_information.channel_name":i},{"_id":False,"Channel_information":True})
        for ch_data in dt1:
            ch_list.append(ch_data["Channel_information"])
    df1=pd.DataFrame(ch_list)


    for index,row in df1.iterrows():
                
        Insert_val_in_col=('''insert into channel(channel_name,
                                                Channel_Id,
                                                Subscription_Count,
                                                Video_count,
                                                Channel_Views,
                                                Channel_Description,
                                                Playlist_Id)
                                                    
                                                value(%s,%s,%s,%s,%s,%s,%s)''')
        
        Value=(row["channel_name"],
        row["Channel_Id"],
        row["Subscription_Count"],
        row["Video_count"],
        row["Channel_Views"],
        row["Channel_Description"],
        row["Playlist_Id"])
        try:
            mycursor.execute(Insert_val_in_col,Value)
            mydb.commit()
            print("Channel values are inserted successfully")
        except:
            print("Channel values are already exist")
            



#creating table for video details in sql database

def video_Table(channelnames):     
     mycursor.execute("drop table if exists video")
     mydb.commit()


     mycursor.execute('''create table if not exists video(channel_name varchar(200),
                                        Channel_Id varchar(200),
                                        Video_Id varchar(200) ,
                                        Video_Name varchar(200),                                    
                                        Thumbnail varchar(200),
                                        Video_Description text,
                                        PublishedAt varchar(100),
                                        View_Count bigint,
                                        Like_Count bigint,
                                        Favorite_Count bigint,
                                        Comment_Count bigint,
                                        Caption_Status varchar(200))''')
     mydb.commit()

     # creat dataframe in pandas
     ls=channelnames
     vi_list=[]
     for i in ls:
        dt3=collect.find({"Channel_information.channel_name":i},{"_id":False,"Video_information":True})
        for vi_data in dt3:
            for i in range(len(vi_data["Video_information"])):
                vi_list.append(vi_data["Video_information"][i])
     df3=pd.DataFrame(vi_list)

     for index,row in df3.iterrows():                    
          Insert_val_in_col='''insert into video(channel_name,
                                                  Channel_Id,
                                                  Video_Id,
                                                  Video_Name,
                                                  Thumbnail,
                                                  Video_Description,
                                                  PublishedAt,
                                                  View_Count,
                                                  Like_Count,
                                                  Favorite_Count,
                                                  Comment_Count,
                                                  Caption_Status)
                                                            
                                                  value(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
          
          Value=(row["channel_name"],
                    row["Channel_Id"],
                    row["Video_Id"],
                    row["Video_Name"],
                    row["Thumbnail"],
                    row["Video_Description"],
                    row["PublishedAt"],
                    row["View_Count"],
                    row["Like_Count"],
                    row["Favorite_Count"],
                    row["Comment_Count"],
                    row["Caption_Status"])
          try:
               mycursor.execute(Insert_val_in_col,Value)
               mydb.commit()
               print("Channel values are inserted successfully")
          except:
               print("Channel values are already exist")
               

#creating table for comment details in sql database

def comment_Table(channelnames):
    mycursor.execute("drop table if exists comment")
    mydb.commit()


    mycursor.execute('''create table if not exists comment(Comment_Id varchar(200) ,
                                                            Video_id varchar(200),
                                                            Comment_Text text,
                                                            Comment_Author varchar(200),
                                                            Comment_PublishedAt varchar(100))''')
    mydb.commit()

    # creat dataframe in pandas
    ls=channelnames
    cm_list=[]
    for i in ls:
        dt4=collect.find({"Channel_information.channel_name":i},{"_id":False,"Comment_information":True})
        for cm_data in dt4:
            for i in range(len(cm_data["Comment_information"])):
                cm_list.append(cm_data["Comment_information"][i])
    df4=pd.DataFrame(cm_list)

    for index,row in df4.iterrows():
        Insert_val = ("insert into comment(Comment_Id,Video_id,Comment_Text,Comment_Author,Comment_PublishedAt) value(%s,%s,%s,%s,%s)")
        val=(row["Comment_Id"],row["Video_id"],row["Comment_Text"],row["Comment_Author"],row["Comment_PublishedAt"])
        mycursor.execute(Insert_val,val)
        mydb.commit()

#creating function a single function to call all the function of sql table creation
def Tables(channelnames):
    Channels_Table(channelnames)
    video_Table(channelnames)
    comment_Table(channelnames)
    return "Data successfully migrated into SQL"


#collecting channel list from mongodb   
ch_list=[]
dt1=collect.find({},{"_id":False,"Channel_information":True})
for ch_data in dt1:
    ch_list.append(ch_data["Channel_information"]["channel_name"])




#creating function to display video information fron mongodm data base and display in streamlit

def Display_vi_tab():
    vi_list=[]
    dt3=collect.find({},{"_id":False,"Video_information":True})
    for vi_data in dt3:
        for i in range(len(vi_data["Video_information"])):
            vi_list.append(vi_data["Video_information"][i])
    df3=st.dataframe(vi_list)
    return df3


#creating function to display comment information fron mongodm data base and display in streamlit

def Display_cm_tab():
    cm_list=[]
    dt4=collect.find({},{"_id":False,"Comment_information":True})
    for cm_data in dt4:
        for i in range(len(cm_data["Comment_information"])):
            cm_list.append(cm_data["Comment_information"][i])
    df4=st.dataframe(cm_list)
    return df4

# creating sidebar in streamlit
with st.sidebar:
    st.title(":rainbow[YouTube Data Harvesting and Warehousing]")
    st.header(":gray[Skills take away From This Project]")
    st.caption("Python scripting")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("Streamlit")    
    st.caption("API integration")
    st.caption("Data Management using MongoDB (Atlas) and SQL")


tab1,tab2,tab3=st.tabs(["Input Channel ID & Import Data into MongoDB","Migrate Data into SQL","Questions"])

with tab1:
    with st.container(border=True):
        #getting channel id from user on streamlit
        Channel_Id=st.text_input("Enter Channel ID :")


        #creating button to store data into mongodb
        if st.button("Store data in a MongoDB"):
            ch_ids=[]
            for i in collect.find({},{"_id":False,"Channel_information":True}):
                ch_ids.append(i["Channel_information"]["Channel_Id"])
            if len(Channel_Id)==0:
                st.error("PLEASE ENTER CHANNEL_ID!!")
            else:
                if Channel_Id not in ch_ids:              
                    insert=channel_information(Channel_Id)
                    st.success(insert)
                    st.balloons()
                else:
                    st.error("Channel details is already exist in MongoDb")


with tab2:
    with st.container(border=True):
        CH_N=st.multiselect("Select Channal Name to Import Data",ch_list)


        #creating button to migrate data from mongodb to sql database  
        if st.button("Migrate data to SQL"):
            if len(CH_N)>0:
                table=Tables(CH_N)
                st.success(table)
                st.balloons()
            else:
                st.error("PLEASE SELECT CHANNEL NAME!!")


import mysql.connector

#sql connection
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="12345",
)

print(mydb)
mycursor = mydb.cursor(buffered=True)



with tab3:
    with st.container(border=True):
        #creating selectbox for display question in streamlit
        question=st.selectbox("Select the question",("1. What are the names of all the videos and their corresponding channels?",
                                                    "2. Which channels have the most number of videos, and how many videos do they have?",
                                                    "3. What are the top 10 most viewed videos and their respective channels?",
                                                    "4. How many comments were made on each video, and what are their corresponding video names?",
                                                    "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                                                    "6. Which channel have the lowest number of subscribers and what is their subscribers & videos count?",
                                                    "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                                                    "8. What are the names of all the channels that have published videos in the year 2022?",
                                                    "9. Which channel has uploaded lowest number of videos",
                                                    "10. Which videos have the highest number of comments, and what are their corresponding channel names?"))

        mycursor.execute("use youtube_project")

        # answers for the questions given in selectbox
        if question=="1. What are the names of all the videos and their corresponding channels?":    
            query1="select Video_Name,channel_name from video"
            mycursor.execute(query1)
            mydb.commit()
            out1=mycursor.fetchall()
            df1=pd.DataFrame(out1,columns=["Video_Name","channel_name"])
            st.write(df1)

        if question=="2. Which channels have the most number of videos, and how many videos do they have?":    
            query2="select Video_count,channel_name from channel order by Video_count desc LIMIT 1"
            mycursor.execute(query2)
            mydb.commit()
            out2=mycursor.fetchall()
            df2=pd.DataFrame(out2,columns=["Video_count","channel_name"])
            st.write(df2)
            
        if question=="3. What are the top 10 most viewed videos and their respective channels?":    
            query3="select View_Count,channel_name from video order by View_Count desc LIMIT 10"
            mycursor.execute(query3)
            mydb.commit()
            out3=mycursor.fetchall()
            df3=pd.DataFrame(out3,columns=["View_Count","channel_name"])
            st.write(df3)

        if question=="4. How many comments were made on each video, and what are their corresponding video names?":
            query4="select Video_Name,Comment_Count from video"
            mycursor.execute(query4)
            mydb.commit()
            out4=mycursor.fetchall()
            df4=pd.DataFrame(out4,columns=["Video_Name","Comment_Count"])
            st.write(df4)
            df4

        if question=="5. Which videos have the highest number of likes, and what are their corresponding channel names?":
            query5="select Video_Name,channel_name,Like_Count from video order by View_Count desc LIMIT 1"
            mycursor.execute(query5)
            mydb.commit()
            out5=mycursor.fetchall()
            df5=pd.DataFrame(out5,columns=["Video_Name","channel_name","Like_Count"])
            st.write(df5)


        if question=="6. Which channel have the lowest number of subscribers and what is their subscribers & videos count?":
            query6="select channel_name,Subscription_Count,Video_count from channel order by Subscription_Count LIMIT 1"
            mycursor.execute(query6)
            mydb.commit()
            out6=mycursor.fetchall()
            df6=pd.DataFrame(out6,columns=["channel_name","Subscription_Count","Video_count"])
            st.write(df6)


        if question=="7. What is the total number of views for each channel, and what are their corresponding channel names?":
            query7="select channel_name,Channel_Views from channel"
            mycursor.execute(query7)
            mydb.commit()
            out7=mycursor.fetchall()
            df7=pd.DataFrame(out7,columns=["channel_name","Channel_Views"])
            st.write(df7)


        if question=="8. What are the names of all the channels that have published videos in the year 2022?":
            query8="select Video_Name,channel_name,PublishedAt from video where year(PublishedAt)=2022"
            mycursor.execute(query8)
            mydb.commit()
            out8=mycursor.fetchall()
            df8=pd.DataFrame(out8,columns=["Video_Name","channel_name","PublishedAt"])
            st.write(df8)


        if question=="9. Which channel has uploaded lowest number of videos":
            query9="select channel_name,Video_Count from channel order by Video_Count LIMIT 1"
            mycursor.execute(query9)
            mydb.commit()
            out9=mycursor.fetchall()
            df9=pd.DataFrame(out9,columns=["channel_name","Video_Count"])
            st.write(df9)


        if question=="10. Which videos have the highest number of comments, and what are their corresponding channel names?":
            query10="select Video_Name,channel_name,Comment_Count from video order by Comment_Count desc LIMIT 1"
            mycursor.execute(query10)
            mydb.commit()
            out10=mycursor.fetchall()
            df10=pd.DataFrame(out10,columns=["Video_Name","channel_name","Comment_Count"])
            st.write(df10)
    
    with st.container(border=True):
      if len(CH_N)>0:
            st.header("BAR-CHART")
            data_1={"channel_name":[],"Video_count":[]}    
            for i in CH_N:
                db_1=collect.find({"Channel_information.channel_name":i},{"_id":False,"Channel_information":True})       
                for i in db_1:
                    data_1["channel_name"].append(i["Channel_information"]["channel_name"])
                    data_1["Video_count"].append(int(i["Channel_information"]["Video_count"]))
                df_1=pd.DataFrame(data_1)
            st.bar_chart(df_1.set_index("channel_name"))
