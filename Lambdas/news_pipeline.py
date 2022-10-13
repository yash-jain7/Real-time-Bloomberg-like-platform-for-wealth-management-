import json
import boto3
import requests
import random
import os
import subprocess
import sys


def func(company):

    r = requests.get(f"https://stocknewsapi.com/api/v1?tickers={company}&items=50&page=1&token=79fb7fjfvna1f3qdia6o02j7dpyyo3j4g6oj9pm7")
    data_stockwits = r.json()
    news_submission = []
    news_submission = [{'Data': json.dumps({'news_url': i['news_url'] , 'title' : i['title'], 'sentiment' : i['sentiment'],  'stock_name': company}).encode('utf-8')} for i in data_stockwits['data']]
    return news_submission

def deliver_stream(data):

    # firehose:us-east-1:521124188609:deliverystream/PUT-S3-x0gdJ
    # client = boto3.client("firehose", region_name="us-east-1")
    client = boto3.client('firehose',
                          region_name='us-east-1',
                          aws_access_key_id='',
                          aws_secret_access_key=''
                          )

    client.put_record_batch(
                DeliveryStreamName="news_stream",
                Records=data
            )

def lambda_handler(event, context):

    company_name = ['AAPL','TSLA','GOOGL','FB','AMZN','NFLX','MSFT','UBER','TCEHY','BABA', 'TWTR']
    for c in company_name:
        submission = func(c)
        deliver_stream(submission)



    return {
    'statusCode': 200,
    'body': json.dumps('Done!')
    }
