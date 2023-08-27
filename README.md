# Spotify dbt Snowflake End-to-End ELT Pipeline
**Full pipeline process**:
![Alt Text](https://github.com/kcao22/spotify_elt/blob/main/Dashboards%20%26%20Visuals/elt_pipeline.png)

**Demo of Looker Studio BI Dashboard**:
![Alt Text](https://github.com/kcao22/spotify_elt/blob/main/Dashboards%20%26%20Visuals/Spotify_Listening_Activity.PNG)

**Project Star Schema Dimensional Model**
![Alt Text](https://github.com/kcao22/spotify_elt/blob/main/Data%20Modeling/spotify_star_schema.png)

## Project Goal
 - To learn how to utilize modern data stack tools to build an ELT pipeline.
 - Create an orchestrated ELT pipeline beginning from data generation, data ingestion, modern tool architectural designing, and end-user data analytics / business intelligence.
   
## Libraries and Resources Used
 - **Python Version**: 3.11
 - **Main Packages**: airflow, pandas, requests, snowflake-connector,spotipy
   
# Pipeline Architecture
## Extract / Load: API Source Data, Extraction, and Load to Snowflake
1. Extracted my own daily listening data from Spotify uisng Spotify's Web API and other Python libraries.
2. Returned JSON objects containing data for the last 50 tracks played are filtered for appropriate data, and primary keys for each row of data (each row represents one song) is generated. Because each song has a unique track ID and is associated with a time played, it was intuitive to combine the track ID along with a standardized time to generate a primary key.
3. Extracted data on tracks played, artist of track played, and album of track played were stored on a Pandas DataFrame.
4. Snowflake-connector library is then used to connect to the RAW database Spotify schema in my Snowflake warehouse and an append new rows strategy is employed by comparing the above-mentioned generated primary keys.

# dbt Transforms: modeling, testing, and building
1. dbt is employed to transform the raw data loaded from local Python script to my RAW database Spotify schema in Snowflake.
2. As the raw table is essentially loaded into the warehouse as a wide table, dbt is necessary for dimensional modeling purposes for ease of understanding of data and accessibility.
3. Data models are built, including a dimension table for tracks, a dimension table on artists, a dimension table on albums, and finally a fact table that serves as a historical capture of listening activity.
4. The dimensional tables contain slowly changing dimensions in the form of popularity and follower fields. **For the purposes and intents of this project, I wanted to capture this data as a SCD type 2**. Because of this, it was necessary to create dimensional models in dbt that would capture a valid_from date to a valid_to date. dbt's snapshots functionality did not work well with duplicate ID values that would be used to determine if an older record should be updated or not. Because of this, it was necessary to create an **incremental model** manually that would only append new data and then refresh the data associated with the previous max date.
5. Once a track, artist, or album reappears in new data (as determined by a date_appended field), the dimensional model scripts will update the now previous max date dimension's valid_to field to the new max date. Then, the newer refreshed data is appended with the date appended date as its valid_from field and a null value for its valid_to field. This way, the dimensional tables capture followers and popularity over time. Current followers and popularity can be filtered for using the valid_to field (equals null).
6. Once the models were finalized, generic and single tests were written into the dbt .yml files. Some tests included asserting ID fields could not be null and the time track primary key (mentioned above) could not contain duplicates. Further, relationship tests were created to ensure appropriate star schema dimensional modeling.
7. In addition to dbt's built in tests, I also wrote other generic tests for certain fields such as popularity fields and follower fields using a combination of SQL and Jinja (see tests/generic directory).

# BI Layer: Looker Studio
1. Looker Studio was chosen given its free accessibility and its prevalence in the current modern data stack.
2. Using the star schema dimensional models built from dbt and loaded into my warehouse's analytics production environment, I blended the data models to generate appropriate relationships in Looker Studio to produce the desired dashboard (see top of readme).
3. Summary statistic cards at the top of the dashboard allow me to track lifetime listening statistics like total unique songs listened to, total listening time, and total distinct artists listened to.
4. Below the summary statistic cards are time-series graphs of popularity of songs listened to over time in addition to the most recent week's (Looker Studio dynamically adjusts the week as set) listening activities (songs listened to by day).
5. Finally, tables to the left show a quick top 10 songs, artists, and albums played as well as the counts of each.

# Orchestration: Apache Airflow and dbt jobs
1. Airflow was used to orchestrate and automate the pipeline process.
2. Scheduled jobs allowed for no manual intervention as data was extracted, loaded, and transformed with Python scripts executed by Airflow at intervals and then finalized dbt models were built into production using dbt's job scheduler.
3. Using python operators, I set up Airflow to run the Python scripts that performed the extract and load phases.
4. Using bash operators, I set up Airflow to run dbt CLI bash commands to call separate portions of the dbt transform process (debug, test raw source data, build stage model, test stage model, build dimensional models, and test dimensional models).
![Alt Text](https://github.com/kcao22/spotify_elt/blob/main/Dashboards%20%26%20Visuals/airflow_dag.PNG)
