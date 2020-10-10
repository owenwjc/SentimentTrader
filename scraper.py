import pymongo
from pymongo import MongoClient
import pandas as pd
import praw
import time
import datetime
from datetime import datetime, timedelta
import re

client = MongoClient('localhost', 27017)
db = client.db
threads = db.threads
secret = db.secret.find_one()['Secret']

reddit = praw.Reddit(client_id='-wpcPIbA7bhlpw', client_secret=secret, user_agent='sentiment')

wantedTags = ['DD', 'Technicals', 'Fundamentals', 'Discussion', 'YOLO', 'Stocks']
subreddit = reddit.subreddit('wallstreetbets')
postlist = []

lastInsertedDate = db.threads.find_one(sort = [('_id',pymongo.DESCENDING)])['Inserted Date']

for post in subreddit.hot(limit = 1000):
    if len(postlist) < 100:
        postTime = post.created_utc
        postDate = datetime.utcfromtimestamp(postTime)

        if postDate > lastInsertedDate and post.link_flair_text in wantedTags:
            postlist.append(post)
    else:
        break

postDf = pd.DataFrame(columns = ['Inserted Date','Title','Body'])

insertedDate = datetime.utcnow()
titles = [post.title for post in postlist]
bodies = [post.selftext for post in postlist]
tags = [post.link_flair_text for post in postlist]


postDf['Title'] = titles
postDf['Body'] = bodies
postDf['Inserted Date'] = insertedDate
postDf['Tag'] = tags
postDf['Label'] = 0
postDf['Stocks'] = 0

def cleanStrings(string):
    return re.sub("[^a-zA-Z0-9./$:,']+", ' ',string)

postDf['Title'] = postDf['Title'].apply(cleanStrings)
postDf['Body'] = postDf['Body'].apply(cleanStrings)

threads.insert_many(postDf.to_dict('records'))