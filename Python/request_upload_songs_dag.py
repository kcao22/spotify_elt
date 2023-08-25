import snowflake.connector
import spotipy
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from helper_functions import get_raw, append_new
from spotipy.oauth2 import SpotifyOAuth

# ID & Secret file
def load_data():
    try:
        # Read credentials
        credentials = pd.read_excel('file:///mnt/d/Documents/Data Projects/logins.xlsx')
        # Set Spotify credentials
        spotify_client_id = credentials['client_id'][0]
        spotify_client_secret = credentials['client_secret'][0]
        spotify_redirect_url = credentials['redirect_url'][0]
        auth_token = credentials['token'][0]
        
        # Set Snowflake credentials
        snowflake_user = credentials['user'][0]
        snowflake_password = credentials['password'][0]
        snowflake_account = credentials['account'][0]
        snowflake_database = credentials['database'][0]
        snowflake_schema = credentials['schema'][0]
        snowflake_warehouse = credentials['warehouse'][0]
        
        # Create spotipy client
        sp = spotipy.Spotify(
        auth_manager=SpotifyOAuth(
            client_id=spotify_client_id,
            client_secret=spotify_client_secret,
            redirect_uri=spotify_redirect_url,
            scope='user-read-recently-played'
            )
        )
        
        # Get recently played data as wide table
        recently_played = sp.current_user_recently_played(limit=50)
        if recently_played:
            try:
                wide_df = get_raw(recent_tracks=recently_played, token=auth_token)
                
                # Load data to raw data table in Snowflake
                con = snowflake.connector.connect(
                    user=snowflake_user,
                    password=snowflake_password,
                    account=snowflake_account,
                    database=snowflake_database,
                    schema=snowflake_schema,
                    warehouse=snowflake_warehouse
                )
                try:
                    append_new(con, wide_df)
                except Exception as e:
                    print('Append new failed')
                    print(e)
            except Exception as e:
                print(e)
    except Exception as e:
        print('Overall function failed.')
        print(e)
    
with DAG(
    'spotify_elt', 
    start_date=datetime(2023, 8, 9),
    schedule_interval='@daily',
    catchup=False,
) as dag:
    # Create task
    task_A = PythonOperator(
        task_id='call_spotify_api',
        python_callable=load_data
    )
    
    task_B = BashOperator(
        task_id='dbt_debug',
        bash_command='''
        cd /mnt/d/Documents/Data\ Projects/spotify_elt/dbt
        dbt debug
        '''
    )
    
    task_C = BashOperator(
        task_id='dbt_test_sources',
        bash_command='''
        cd /mnt/d/Documents/Data\ Projects/spotify_elt/dbt
        dbt test --select source:*
        '''
    )

    task_D = BashOperator(
        task_id='dbt_run_stage',
        bash_command='''
        cd /mnt/d/Documents/Data\ Projects/spotify_elt/dbt
        dbt run --select stg_daily
        '''
    )
    
    task_E = BashOperator(
        task_id='dbt_test_stage',
        bash_command='''
        cd /mnt/d/Documents/Data\ Projects/spotify_elt/dbt
        dbt test --select stg_daily
        '''
    )

    task_F = BashOperator(
        task_id='dbt_run_star_schema',
        bash_command='''
        cd /mnt/d/Documents/Data\ Projects/spotify_elt/dbt
        dbt run --select dim_albums dim_artists dim_tracks fact_playlog
        '''
    )
    
    task_G = BashOperator(
        task_id='dbt_test_star_schema',
        bash_command='''
        cd /mnt/d/Documents/Data\ Projects/spotify_elt/dbt
        dbt test --select dim_albums dim_artists dim_tracks fact_playlog
        '''
    )

task_A >> task_B >> task_C >> task_D >> task_E >> task_F >> task_G