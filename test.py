import googleapiclient.discovery
import pymongo
import pandas as pd
from sqlalchemy import create_engine
import pymysql

youtube = googleapiclient.discovery.build('youtube', 'v3', developerKey='AIzaSyDXVRqphxsurHIBBOble4U5yUom8DxWUvU')

def get_channel_data(channel_id):
    # Fetch channel details
    channel_response = youtube.channels().list(
        part='snippet, statistics',
        id=channel_id
    ).execute()
    
    # Extract relevant information
    channel_data = {
        'channel_name': channel_response['items'][0]['snippet']['title'],
        'subscribers': channel_response['items'][0]['statistics']['subscriberCount'],
        'total_videos': channel_response['items'][0]['statistics']['videoCount']
        # Add more fields as needed
    }
    
    return channel_data

# Function to retrieve video data
def get_video_data(channel_id):
    playlist_response = youtube.channels().list(
        part='contentDetails',
        id=channel_id
    ).execute()
    
    playlist_id = playlist_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    
    # Fetch video details
    video_response = youtube.playlistItems().list(
        part='snippet',
        playlistId=playlist_id,
        maxResults=50
    ).execute()
    
    videos = video_response['items']
    video_data = []
    
    for video in videos:
        video_data.append({
            'video_id': video['snippet']['resourceId']['videoId'],
            'title': video['snippet']['title'],
            'description': video['snippet']['description'],
            # Add more fields as needed
        })
    
    return video_data

# Example usage
channel_id = ['UCfUjCJCvDmfx20BCQZhZ_dg','UCaB7Ze737I13it_QskH-WBw',
            'UCqBudXta5bDRCjLYWTGFJ-Q','UCECR7FBx9I9CH7698rxYgFQ',
            'UChv_CgPkCDHSMxHgcoknD-w','UCPlUuamt8AArXAevFWUiUKw',
            'UCTCMjShTpZg96cXloCO9q1w','UCvppRuz_VW0x2a5BEAphhNg',
            'UC81xlBzgX5JZfTjHS3kg-VA','UCk3JZr7eS3pg5AGEvBdEvFg']
for ids in channel_id:

    channel_data = get_channel_data(ids)
    video_data = get_video_data(ids)
    print(video_data,channel_data)
    client = pymongo.MongoClient('mongodb://localhost:27017/')
    db = client['database_ytube']
    collection = db['collection_ydata']

    # # Insert channel data into MongoDB
    collection.insert_one(channel_data)

    # Insert video data into MongoDB
    collection.insert_many(video_data)


print("Data stored in Mongodb")


# Connect to SQL database
connection_string = 'mysql+pymysql://root:Alarum$999@localhost:3306/youtubeextract'

engine = create_engine(connection_string)

# Migrate channel data to SQL
channel_df = pd.DataFrame(list(collection.find()))
channel_df.to_sql('channels', engine, if_exists='replace', index=False)

# Migrate video data to SQL
video_df = pd.DataFrame(list(collection.find()))
video_df.to_sql('videos', engine, if_exists='replace', index=False)

print("Data stored in MySql")
