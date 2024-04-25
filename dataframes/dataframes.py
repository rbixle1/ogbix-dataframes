import json
import boto3
from datetime import datetime, timedelta
from song_stemmer import key_builder
import pandas as pd
import io


START_DATE = datetime.now() 
dynamodb = boto3.resource('dynamodb')

def fmt_response(status_code, body):  
    return {
        'statusCode': status_code,
        'headers': {
            "Access-Control-Allow-Headers" : "Content-Type,Type,Authorization,X-Amz-Date,X-Api-Key,X-Amz-Security-Token",
            "Access-Control-Allow-Origin": "*", 
            "Access-Control-Allow-Methods": "OPTIONS,POST" 
        },
        'body':  body
    }

def get_days(days_to_process):
    date_list = [] 
    for n in range(days_to_process):
        date_list.append(datetime.strftime((START_DATE - timedelta(n)), '%m-%d-%Y'))
    return date_list


def get_list(city, date):
    client = boto3.client('s3', region_name='us-west-2')
    paginator = client.get_paginator('list_objects_v2')


    s3 = boto3.resource('s3')
    song_list = {}

    page_iterator = paginator.paginate(Bucket='tracks.json',
            Prefix="tracks/" + city + "/"+ date + "/") # only this city
    for page in page_iterator:

        # Aggregate the bucket
        print('Aggregating songs....:' + date)
        try:
            for obj in page['Contents']:
                song_bucket = obj['Key']
            
                track = s3.Object('tracks.json', song_bucket)
                
                song = json.loads(track.get()['Body'].read())
                #print(song)

                # Get the songs hash key
                key = key_builder( song['song'] + ' ' + song['band'])

                # Creat a list of occurances of the song.
                if key in song_list :
                    song_list[key][0] = song_list[key][0] + 1
                else:
                    song_list.update({key: [1, song['song'], song['band'], key, city, date]})
        except KeyError as err:
            print('Error: ' + str(err)) 
    return song_list

def handler(event, context):
    print(pd.__version__)

    # loop through cities
    dyanamodb = boto3.resource('dynamodb')
    all_stations = dyanamodb.Table('Stations')
    stations = all_stations.scan()
    cities = [] # Get cities to process from DynamoDB Stations table.
    cities.extend(stations.get("Items", []))



    global_list = {}
    days = get_days(1)
    for day in days:
        combined_df = pd.DataFrame()
        for city in cities:
            print('Processing: ' + city['City'].replace(' ', '') + ' ...')
            song_list = get_list(city['City'].replace(' ', ''),day)
            df = pd.DataFrame(song_list.values())
            combined_df = pd.concat([combined_df, df])
            print(df)
        #Save todays bucket    
        combined_df.columns = ['count', 'song', 'artist', 'key', 'city', 'date']
        bucket = 'daily-statistics'
        destination = day + '/aggregated-daily.json'
        json_buffer = io.StringIO()
        combined_df.to_json(json_buffer,orient='records')
        s3 = boto3.resource('s3')
        s3_bucket = s3.Bucket(bucket)
        s3_bucket.put_object(Key=destination, Body=json_buffer.getvalue())
        return fmt_response(200, 'processed')