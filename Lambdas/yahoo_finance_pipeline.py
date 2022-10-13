import json
import boto3
import random
import os
import subprocess
import sys

def install(package):
    subprocess.check_call([
        sys.executable,
        "-m",
        "pip",
        "install",
        "--target",
        "/tmp",
        package])

install('yfinance')

sys.path.append('/tmp')

import yfinance as yf

def yahoo_data(stock):

    start_date = datetime.now() - datetime.timedelta(days=6)
    start_date = start_date.strftime('%Y-%m-%d')

    end_date = datetime.now() - datetime.timedelta(days=1)
    end_date = end_date.strftime('%Y-%m-%d')

    company_ticker = yf.Ticker(stock)
    history = company_ticker.history(start=start_date, end=end_date)

    data = []

    for index, rows in history.iterrows():
        data += [{
                "high": rows['High'],
                "low": rows['Low'],
                "open": rows['Open'],
                'close': rows['Close'],
                "ts": str(index),
                "name": stock
                }]

    stock_submission = []
    stock_submission = [{'Data': json.dumps({'high': i['high'] , 'open': i['open'], 'low' : i['low'],
    'ts' : i['ts'],  'stock_name': i['name']}).encode('utf-8')} for i in data]
    return stock_submission



def deliver_stream(data):
    client = boto3.client('firehose',
                          region_name='us-east-1',
                          aws_access_key_id='',
                          aws_secret_access_key=''
                          )

    client.put_record_batch(
                DeliveryStreamName="stock_price_stream",
                Records=data,
            )


def lambda_handler(event, context):

    company_name = ['AAPL','TSLA','GOOGL','FB','AMZN','NFLX','MSFT','UBER','TCEHY','BABA', 'TWTR']
    for c in company_name:
        submission = yahoo_data(c)
        deliver_stream(submission)


    return {
    'statusCode': 200,
    'body': json.dumps('Done!')
    }
