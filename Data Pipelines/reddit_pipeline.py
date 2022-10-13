import json
import requests
from psaw import PushshiftAPI
import datetime as dt
import boto3


def get_reddit_submission(company_name):

    api = PushshiftAPI()
    submission = list(api.search_submissions(q = company_name, subreddit='wallstreetbets', filter=['title'], limit=100))
    text_submission = []
    for i in submission:
        if i.title != '' or i.title != '[removed]\n':
            text_submission.append(i.title)
    text_submission = [{'Data': json.dumps({'Data': i , 'company_name': company_name}, ensure_ascii = True) +'\n'}  for i in
                  text_submission]

    return text_submission

def get_reddit_comment(company_name):

    api = PushshiftAPI()
    gen = api.search_comments(q=company_name,subreddit='wallstreetbets',filter=['body'],limit=100)
    text_comment = []
    comment = (list(gen))
    for i in comment:
        if i.body != '':
            text_comment.append(i.body)
    text_comment = [{'Data': json.dumps({'Data': i , 'company_name': company_name}, ensure_ascii = True) + '\n'}  for i in
                  text_comment]

    return text_comment
