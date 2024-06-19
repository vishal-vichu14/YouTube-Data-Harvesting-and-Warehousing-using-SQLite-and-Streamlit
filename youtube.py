import pandas as pd
import streamlit as sl
import googleapiclient.discovery
from streamlit_option_menu import option_menu
apid = "AIzaSyATcpRHy0URlwKS_sj0MN3PHk7Y-5xODZ0"
api_service_name = "youtube"
api_version = "v3"
youtube = googleapiclient.discovery.build(
    api_service_name, api_version, developerKey=apid)

def get_channel_id(channel_id): #function to get channel details.
  request = youtube.channels().list(
      part="statistics,snippet,contentDetails",
      id=channel_id
  )
  response = request.execute()

  for i in response["items"]:
    info = dict(Channel_name =i["snippet"]["title"],
              Channel_id=i["id"],
              Subscription_Count=i["statistics"]["subscriberCount"],
              Channel_viwes=i["statistics"]["viewCount"],
              Channel_Description=i["snippet"]["description"],
              Playlist_id=i["contentDetails"]["relatedPlaylists"]["uploads"])
  cd=pd.DataFrame(info,index=[0])
  return cd

def get_vid_id(channel_id): #function to get all video ids.
  vid_ids=[]
  channel_response= youtube.channels().list(id=channel_id,
                                    part="contentDetails").execute()
  playlist_id=channel_response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
  next_page_token=None
  while True:
    request = youtube.playlistItems().list(
        part="snippet",
        playlistId=playlist_id,maxResults=50,
        pageToken=next_page_token
    )
    vid_response = request.execute()
    for i in range(len(vid_response["items"])):
      vid_ids.append(vid_response["items"][i]["snippet"]["resourceId"]["videoId"])
    next_page_token=vid_response.get("nextPageToken")
    if next_page_token==None:
      break
  return vid_ids


def get_vid_info(video_ids): #function to get video details.
  all_videos_info=[]
  for ids in video_ids:
    request = youtube.videos().list(
      part="contentDetails,snippet,statistics",
      id=ids
    )
    response = request.execute()
    for i in response["items"]:
      video_info=dict(Channel_name=i["snippet"]["channelTitle"],
                      Channel_id=i["snippet"]["channelId"],
                      Video_id=i["id"],
                      Video_name=i["snippet"]["title"],
                      Video_Description=i["snippet"]["description"],
                      PublishedAt=pd.to_datetime(i["snippet"]["publishedAt"]),
                      View_Count=i["statistics"]["viewCount"],
                      Like_Count=i["statistics"]["likeCount"],
                      Comment_Count=i["statistics"].get("commentCount"),
                      Duration=i["contentDetails"]["duration"],
                      Thumbnail=i["snippet"]["thumbnails"]['default']['url'],
                      Caption_Status=i["contentDetails"]["caption"]
                      )
      all_videos_info.append(video_info)
  av=pd.DataFrame(all_videos_info)
  return av

def get_comments_info(video_ids): #function to get comment details
  comments_info=[]
  try:
    for ids in video_ids:
      request = youtube.commentThreads().list(
        part="snippet",
        videoId=ids,
        maxResults=50
      )
      response = request.execute()
      for i in response["items"]:
        comments_data=dict(Comment_id=i["snippet"]["topLevelComment"]["id"],
                          Channel_id=i["snippet"]["channelId"],
                          video_id=i["snippet"]["topLevelComment"]["snippet"]["videoId"],
                          Comment_Text=i["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                          Comment_Author=i["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                          Comment_PublishedAt=i["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                          )
        comments_info.append(comments_data)
      ci=pd.DataFrame(comments_info)
  except:
    pass
  return ci

#sqlite connectivity
import sqlite3
conn=sqlite3.connect('harvest.db')
cursor=conn.cursor()


def all_functions(channelid): #main function which collects the data and stores them in SQLite database
  message="Successfully Stored"
  channels=get_channel_id(channelid)
  videoids=get_vid_id(channelid)
  videos=get_vid_info(videoids)
  comments=get_comments_info(videoids)
  channels.to_sql("Channels", conn, if_exists="append", index=False)
  videos.to_sql("Videos", conn, if_exists="append", index=False)
  comments.to_sql("Comments", conn, if_exists="append", index=False)
  return message


from isodate import parse_duration
def dur(duration_str): #function to avoid time formate problem in video duration
    duration_seconds = parse_duration(duration_str).total_seconds()
    return duration_seconds

#SQL query to retrive data from SQLite database
ch=pd.read_sql_query('''SELECT Channel_name as 'Channel name',Subscription_Count as 'Subscription Count', Channel_Description as 'Channel Description' from Channels''',conn)
vid=pd.read_sql_query('''SELECT Channel_name as 'Channel name',Video_name as 'Video name', View_Count as 'View Count'  from Videos''',conn)
com=pd.read_sql_query('''SELECT Comment_Text as 'Comment text',Comment_Author as 'Comment author' from Comments''',conn)
cha=ch.drop_duplicates()
#"1.What are the names of all the videos and their corresponding channels?"
question1=pd.read_sql_query('''SELECT Channel_name as "CHANNEL NAME",Video_name as "VIDEOS NAME" from Videos''',conn)
#"2.Which channels have the most number of videos and how many videos do they have?",
question2=pd.read_sql_query('''select Channel_name as "CHANNEL NAME", count(*) as "NUMBER OF VIDEOS" from Videos GROUP BY Channel_name ORDER BY count(*) desc''',conn)
#"3.What are the top 10 most viewed videos and their respective channels?"
question3=pd.read_sql_query('''SELECT Channel_name as "CHANNEL NAME",Video_name as "VIDEO NAME",View_Count as "No.of VIEWS" from Videos ORDER BY View_Count desc LIMIT 10''',conn)
#"4.How many comments were made on each video, and what are their corresponding video names?"
question4=pd.read_sql_query('''SELECT Video_name as "VIDEOS NAME",Comment_Count as "No.of COMMENTS" from Videos''',conn)
#"5.Which videos have the highest number of likes, and what are their corresponding channel names?"
question5=pd.read_sql_query('''SELECT Channel_name as "CHANNEL NAME",Video_name as "VIDEOS NAME",Like_Count as "No.of LIKES" from Videos ORDER BY Like_Count desc''',conn)
#"6.What is the total number of likes for each video and what are their corresponding video names?"
question6=pd.read_sql_query('''SELECT Video_name as "VIDEOS NAME",Like_Count as "No.of LIKES" from Videos ORDER BY Like_Count desc''',conn)
#"7.What is the total number of views for each channel, and what are their corresponding channel names?",
question7=pd.read_sql_query('''SELECT Channel_name as "CHANNEL NAME",Channel_viwes as "No.of VIEWS" from Channels ORDER BY Channel_viwes desc''',conn)
#"8.What are the names of all the channels that have published videos in the year 2022?"
question8=pd.read_sql_query('''SELECT Channel_name as "CHANNEL NAME",Video_name as "VIDEO NAME",PublishedAt as "Published At" from Videos WHERE strftime('%Y',PublishedAt) = '2022' ''',conn)
#"9.What is the average duration of all videos in each channel, and what are their corresponding channel names?"
question9=pd.read_sql_query('''SELECT Channel_name as "CHANNEL NAME",Duration as "DURATION" from Videos''',conn)
question9["dur_alter"]=question9["DURATION"].apply(dur)
question9= question9.drop('DURATION', axis=1)
question9= question9.rename(columns={'dur_alter': 'DURATION in sec'})
q90 = question9.groupby('CHANNEL NAME').mean()
#"10.Which videos have the highest number of comments, and what are their corresponding channel names?"
question10=pd.read_sql_query('''select Channel_name as "CHANNEL NAME",Video_name as "VIDEO NAME",Comment_Count as "No.of COMMENTS " from Videos WHERE Comment_Count is not null ORDER BY Comment_Count desc''',conn)

#code for Streamlit Application

sl.title(":green[YOUTUBE DATA HARVESTING AND WAREHOUSING]")

with sl.sidebar:
    selected = option_menu("Main Menu", ["HOME", 'VIEW'],
        icons=['house', 'eye'], menu_icon="cast", default_index=0)
if selected == "HOME":
  channelid=sl.text_input("Enter the Youtube Channel Id")
  collect=sl.button(":green[Collect and store in SQL]")
  if collect:
      all_functions(channelid)
      sl.write("Successfully uploaded")
  with sl.expander("CHANNELS"):
      sl.write(cha)
  with sl.expander("VIDEOS"):
      sl.write(vid)
  with sl.expander("COMMENTS"):
      sl.write(com)
elif selected == "VIEW":
    question = sl.sidebar.selectbox("Select Questions",
                                    ("1.What are the names of all the videos and their corresponding channels?",
                                     "2.Which channels have the most number of videos and how many videos do they have?",
                                     "3.What are the top 10 most viewed videos and their respective channels?",
                                     "4.How many comments were made on each video, and what are their corresponding video names?",
                                     "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                                     "6.What is the total number of likes for each video, and what are their corresponding video names?",
                                     "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                                     "8.What are the names of all the channels that have published videos in the year 2022?",
                                     "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                     "10.Which videos have the highest number of comments, and what are their corresponding channel names?"))

    if question == "1.What are the names of all the videos and their corresponding channels?":
        q1 = pd.DataFrame(question1)
        sl.write(q1)
    elif question == "2.Which channels have the most number of videos and how many videos do they have?":
        q2 = pd.DataFrame(question2)
        sl.write(q2)
    elif question == "3.What are the top 10 most viewed videos and their respective channels?":
        q3 = pd.DataFrame(question3)
        sl.write(q3)
    elif question == "4.How many comments were made on each video, and what are their corresponding video names?":
        q4 = pd.DataFrame(question4)
        sl.write(q4)
    elif question == "5.Which videos have the highest number of likes, and what are their corresponding channel names?":
        q5 = pd.DataFrame(question5)
        sl.write(q5)
    elif question == "6.What is the total number of likes for each video, and what are their corresponding video names?":
        q6 = pd.DataFrame(question6)
        sl.write(q6)
    elif question == "7.What is the total number of views for each channel, and what are their corresponding channel names?":
        q7 = pd.DataFrame(question7)
        sl.write(q7)
    elif question == "8.What are the names of all the channels that have published videos in the year 2022?":
        q8 = pd.DataFrame(question8)
        sl.write(q8)
    elif question == "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        q9 = pd.DataFrame(q90)
        sl.write(q9)
    elif question == "10.Which videos have the highest number of comments, and what are their corresponding channel names?":
        q10 = pd.DataFrame(question10)
        sl.write(q10)


