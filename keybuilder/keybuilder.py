import json
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
import pandas as pd
from playwright.sync_api import sync_playwright, Playwright

HEADLESS = False
START_DATE = datetime.now()
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client("s3")
city_chart = dynamodb.Table('Charts')
keys_table = dynamodb.Table('Keys')


def get_days(days_to_process): 
    date_list = [] 
    for n in range(days_to_process):
        date_list.append(datetime.strftime((START_DATE - timedelta(n)), '%m-%d-%Y'))
    return date_list

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


# # If you found a key insert it into the database
# def add_keys(key, ids, song, band):
#     dyanamodb = boto3.resource('dynamodb')
#     keys_table = dyanamodb.Table('Keys')
#     print("Adding Keys....")
#     for id in ids:
#         try:
#             keys_table.put_item(
#                 Item={
#                     "song_hash": key,
#                     "video": id,
#                     "track": {"song": song, "band": band},
#                 }
#             )
#         except ClientError as err:
#             print(
#                 "Couldn't key %s to table %s. Here's why: %s: %s",
#                 id,
#                 'Keys',
#                 err.response["Error"]["Code"],
#                 err.response["Error"]["Message"],
#             )

def first_search(page):
    find_results = page.locator('cite:has-text("https://www.youtube.com/watch?v=")')
    # get the number of elements/tags
    count = find_results.count()
    ids = []
    # loop through all elements/tags
    for i in range(count):
        # get the element/tag
        element = find_results.nth(i)
        # get the text of the element/tag
        ids.append(element.inner_text().removeprefix('https://www.youtube.com/watch?v='))
    return ids

# Search and scrape to get the data for the video key.
def find_video_key(playwright: Playwright, song, artist, key):
    print("Searching for video key.... Band: " + artist + " Song: " + song) 
    webkit = playwright.webkit
    #browser = webkit.launch(headless = HEADLESS, slow_mo=5000)
    browser = webkit.launch(headless = HEADLESS, slow_mo=5000)
    context = browser.new_context()
    page = context.new_page()
    page.goto(" https://www.bing.com/search?q=" + song + ' ' + artist + ' youtube (official video) ')
    
    ids = first_search(page)

    if(len(ids) > 0):
        print(ids)
        #add_keys(key, ids, song, band)

    browser.close()
    
    
def check_for_video_key(song, artist, key):

   #print("Checking for video key...." + song + ' ' + artist + ' : ' + key)
    try:
        response = keys_table.query(KeyConditionExpression=Key('song_hash').eq(key))
    except ClientError as err:
        print(
            "Couldn't get item %s from table %s. Here's why: %s: %s",
            key,
            'Keys',
            err.response["Error"]["Code"],
            err.response["Error"]["Message"],
        )    
        return False
    if response['Count'] > 0:
        return True
    return False

def process_songs(df):
    local_df = df.groupby(['key', 'song', 'artist']).sum(['count'])
    local_df = local_df.sort_values(by=['count'], ascending=False).reset_index()
    x = 0
    y = 0

    for index, item in local_df.iterrows():
        x = x + 1

        if check_for_video_key(item['song'], item['artist'], item['key']) == False:
            y = y + 1
            with sync_playwright() as playwright: 
                find_video_key(playwright, item['song'], item['artist'], item['key'])
    print('Total not found: ' + str(y))            
    
    
def handler(event, context):
    # Aggregate N days of stats as df
    days = get_days(1)
    
    print('Processing days ... ' + str(days))
    
    df = pd.DataFrame()
    for date in days:
        object_key = date + "/aggregated-daily.json"
        file_content = s3_client.get_object(
            Bucket='daily-statistics', Key=object_key)
        df = pd.concat([df, pd.read_json(file_content['Body'])])
        
    df['song']= df['song'].str.title()
    df['artist']= df['artist'].str.title()

    print('Processing: keys ...')     
    process_songs(df) 


    return fmt_response(200, 'processed')


handler('','')