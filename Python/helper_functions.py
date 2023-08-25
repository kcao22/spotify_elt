import dateutil.parser as dp
import pandas as pd
import pytz
import requests
import spotipy
from datetime import datetime
from snowflake.connector.pandas_tools import write_pandas
from spotipy.oauth2 import SpotifyOAuth

# Create spotipy client object
def create_spotipy_client(client_id, client_secret, redirect_uri, scope):
    '''
    Creates spotipy client object with authorization access to user's designated Spotipy application. Returns spotipy client object.

    ARGUMENTS:
        client_id: User's app client_id as seen via Spotify dashboard.
        client_secret: User's app client_secret as seen via Spotify dashboard.
        redirect_uri: Redirect URL used for acquiring authorization code.
        scope: Scope of accessibility for user's account.
    RETURNS:
        Spotipy client object
    '''
    sp = spotipy.Spotify(
        auth_manager=SpotifyOAuth(
            client_id=client_id,
            client_secret=client_secret,
            redirect_uri=redirect_uri,
            scope=scope
        )
    )
    return sp


# Request user's most recently played songs
def get_recent_played_tracks(spotipy_client, limit):
    '''
    Gets 50 most recently played songs from user's Spotify account. Spotipy will automatically grab authorization code, swap for access token, and contains a method that returns 50 most recently played songs from user's account. Returns user's most recently played tracks in JSON format.

    ARGUMENTS:    
        spotipy_client: Spotipy client object.
        limit: The number of songs recently played that the user would like to return.
    RETURNS:
        JSON object with information of 50 most recently played tracks
    '''
    return spotipy_client.current_user_recently_played(limit=limit)
    

# Directly acquire access token for direct Spotify Web API functionality. For this project, this is used for additional artist and album details such as popularity.
def get_access_token():
    '''
    Uses Spotipy customized cache handler to acquire access token directly. Returns access token.

    ARGUMENTS:
        None
    RETURNS:
        String token for additional request functionality
    '''
    handler = spotipy.CacheFileHandler()
    return handler.get_cached_token()['access_token']


# Access additional details of specified track's artist and album
def additional_info(token, artist_id, album_id):
    '''
    Interacts directly with Spotify Web API using access token associated with spotipy client object to acquire additional information for albums.
    Returns artist followers, artist popularity, album popularity, album total tracks, and album release date.

    ARGUMENTS:
        token: Access token required to access user's Spotify app.
        album_id: Unique Spotify ID associated with album of most recent song played.
        artist_id: Unique Spotify ID associated with artist of most recent song played.
    RETURNS:
        List of additional information for artist and albums.
    '''
    # Authentican headers
    auth_headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }
    # Concatenate specific artist and album IDs to get request URL
    get_artist_info_url = f'https://api.spotify.com/v1/artists/{artist_id}'
    get_album_info_url = f'https://api.spotify.com/v1/albums/{album_id}'
    # Returned JSON data on specified artist and album
    artist_info = requests.get(get_artist_info_url, headers=auth_headers).json()
    album_info = requests.get(get_album_info_url, headers=auth_headers).json()
    # print('Additional metrics')
    # print(f'Artist Info: {artist_info}')
    # print(f'Album Info: {album_info}')
    # Additional artist metrics
    artist_followers = artist_info['followers']['total']
    artist_popularity = artist_info['popularity']
    # print('Album information')
    # Additional album metrics
    album_popularity = album_info['popularity']
    album_total_tracks = album_info['total_tracks']
    album_release_date = album_info['release_date']

    # Returned values
    return artist_followers, artist_popularity, album_popularity, album_total_tracks, album_release_date

def get_raw(recent_tracks, token):
    '''
    Parses out data from recent tracks JSON object into Pandas DataFrame. 
    Returns Pandas DataFrame.
    
    ARGUMENTS:
        recent_tracks: JSON object with recentlty listened to track details.
        token: Authorization token for accessing additional information.
    RETURNS:
        Pandas DataFrame with raw data.

    '''
    # Traversing tracks and storing data in Pandas DataFrame to load to Snowflake
    # Performing transformations on timezones, date formats during data collection
    wide_table_data = []
    curr_date = datetime.today().date()
    for i in range(len(recent_tracks['items'])):
        # Fact table data
        track_id = recent_tracks['items'][i]['track']['id']
        artist_id = recent_tracks['items'][i]['track']['album']['artists'][0]['id']
        album_id = recent_tracks['items'][i]['track']['album']['id']
        track_name = recent_tracks['items'][i]['track']['name']
        track_url = recent_tracks['items'][i]['track']['external_urls']['spotify']
        track_length_ms = recent_tracks['items'][i]['track']['duration_ms']
        track_popularity = recent_tracks['items'][i]['track']['popularity']
        played_at = pytz.utc.localize(datetime.strptime(recent_tracks['items'][i]['played_at'], '%Y-%m-%dT%H:%M:%S.%fZ')).astimezone(pytz.timezone('US/Pacific')).date()
        played_at_unix = str(dp.parse(recent_tracks['items'][i]['played_at']).timestamp()).replace('.', '')  # Conversion from Standard ISO 8610 datetime to UNIX seconds
        unique_id = played_at_unix + track_id

        # Artist data
        artist_name = recent_tracks['items'][i]['track']['album']['artists'][0]['name']
        artist_url = recent_tracks['items'][i]['track']['album']['artists'][0]['external_urls']['spotify']
        
        # Album data
        album_name = recent_tracks['items'][i]['track']['album']['name']
        album_url = recent_tracks['items'][i]['track']['album']['external_urls']['spotify']
        
        # print(f'Track ID: {track_id}')
        # print(f'Artist ID: {artist_id}')
        # print(f'Album ID: {album_id}')
        # print(f'Track Name: {track_name}')
        # print(f'Track URL: {track_url}')
        # print(f'Track Length (MS): {track_length_ms}')
        # print(f'Track Popularity: {track_popularity}')
        # print(f'Played At: {played_at}')
        # print(f'Played At Unix: {played_at_unix}')
        # print(f'Unique ID: {unique_id}')
        # print(f'Artist Name: {artist_name}')
        # print(f'Artist URL: {artist_url}')
        # print(f'Album Name: {album_name}')
        # print(f'Album URL: {album_url}')

        # Additional artist and album data
        # print('Accessing Additional Information')
        artist_followers, artist_popularity, album_popularity, album_total_tracks, album_release_date = additional_info(token, artist_id=artist_id, album_id=album_id)

        # Append data to single wide table
        wide_table_data.append(
            {
                'TIME_TRACK_KEY': unique_id, 
                'TRACK_ID': track_id,
                'TRACK_NAME': track_name,
                'TRACK_URL': track_url,
                'TRACK_LENGTH_MS': track_length_ms,
                'TRACK_POPULARITY': track_popularity,
                'ARTIST_ID': artist_id,
                'ARTIST_NAME': artist_name,
                'ARTIST_URL': artist_url,
                'ARTIST_FOLLOWERS': artist_followers,
                'ARTIST_POPULARITY': artist_popularity,
                'ALBUM_ID': album_id,
                'ALBUM_NAME': album_name,
                'ALBUM_URL': album_url,
                'ALBUM_POPULARITY': album_popularity,
                'ALBUM_TOTAL_TRACKS': album_total_tracks,
                'ALBUM_RELEASE_DATE': album_release_date,
                'PLAYED_AT': played_at, 
                'DATE_APPENDED': curr_date
            }
        )
    return pd.DataFrame(wide_table_data)

def append_new(con, df):
    '''
    Checks existing raw data table and only appends new data from user's recently played tracks.
    
    ARGUMENTS:  
        con: Snowflake connector object.
        df: Pandas DataFrame with recently played tracks.
    '''
    df_current = None
    with con.cursor() as cursor:
        cursor.execute('SELECT TIME_TRACK_KEY FROM RAW.SPOTIFY.DAILY')
        df_current = cursor.fetch_pandas_all()
    unique_ids = set(df_current['TIME_TRACK_KEY'])
    df_new = df[~df['TIME_TRACK_KEY'].isin(unique_ids)]
    write_pandas(conn=con, df=df_new, table_name='DAILY', database='RAW', schema='SPOTIFY')
    
    return