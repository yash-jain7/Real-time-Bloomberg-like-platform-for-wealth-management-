import base64
import requests
import json
import boto3
import time
import datetime
import os
import re
import re
import math
import numpy as np
import tweepy

    # Define your keys from the twitter developer portal


def get_company_tweet(company_name):
    # Define your keys from the twitter developer portal
    client_key = ''
    client_secret = ''



    # Reformat the keys and encode them

    # we start in unicode and then transform to bytes
    key_secret = '{}:{}'.format(client_key, client_secret).encode('ascii')

    # Transform from bytes to bytes that can be printed
    b64_encoded_key = base64.b64encode(key_secret)

    # Transform from bytes back into Unicode
    b64_encoded_key = b64_encoded_key.decode('ascii')

    base_url = 'https://api.twitter.com/'
    auth_url = '{}oauth2/token'.format(base_url)

    auth_headers = {
        'Authorization': 'Basic {}'.format(b64_encoded_key),
        'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8'
    }

    auth_data = {
        'grant_type': 'client_credentials'
    }

    auth_resp = requests.post(auth_url, headers=auth_headers, data=auth_data)

    access_token = auth_resp.json()['access_token']

    search_headers = {
        'Authorization': 'Bearer {}'.format(access_token)
    }

    search_params = {
        'q': company_name,  # + since + until,
        'result_type': 'recent',
        'tweet_mode': 'extended',
        'count': 1000
    }

    search_url = '{}1.1/search/tweets.json'.format(base_url)

    search_resp = requests.get(search_url, headers=search_headers, params=search_params)

    now = datetime.datetime.now()
    format_now = datetime.datetime.strftime(now, '%Y-%m-%d %H:%M:%S')

    Data = json.loads(search_resp.content)
    records = list(
        map((lambda x: {"Data": json.dumps({'datetime': format_now, 'source': 'twitter', 'text': x.get('full_text'),
                                            'location': x.get('user').get('location'),
                                            'user': x.get('user').get('screen_name')}, ensure_ascii=False)}),
            Data.get('statuses')))

    print('{} new records. '.format(len(records)))
    if len(records) <= 0:
        print(search_params)

    res = []
    for sentence in records:
        sentence = json.loads(sentence['Data'])['text']
        sentence = re.sub("[^a-zA-Z0-9]+", " ", sentence)
        sentence = re.sub("^RT", " ", sentence)
        #sentence = re.sub("RT", "", sentence)
        sentence = ' '.join(sentence.strip().split('\n'))
        if sentence and len(sentence.strip().split()) >= 5:
            res.append(sentence.strip())

    final = [{'Data': json.dumps({'Data': i + "\n", 'company_name': company_name}).encode('utf-8')} for i in
                  res]
    return final


def deliver_stream(data):
    # client = boto3.client("firehose", region_name="us-east-1")
    client = boto3.client('firehose',
                          region_name='us-east-1',
                          aws_access_key_id='',
                          aws_secret_access_key=''
                          )

    client.put_record_batch(
                DeliveryStreamName="twitter_data",
                Records=data,
            )



def lambda_handler(event, context):


    company_name = ['AAPL','TSLA','GOOGL','FB','AMZN','NFLX','MSFT','UBER','TCEHY','BABA', 'TWTR']
    for c in company_name:
        submission = get_company_tweet(c)
        deliver_stream(submission)



    return {
        'statusCode': 200,
        'body': json.dumps('success')
    }
