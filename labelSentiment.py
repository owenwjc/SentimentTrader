import pandas as pd
import re

from flair.models import TextClassifier
from flair.data import Sentence
from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client.db
comments = db.comments

commentdf = pd.DataFrame.from_records(comments.find({'$and': [{'sentiment': {'$eq': 0}},
						    {'created_utc': {'$exists': True}}]}))

print(commentdf.head())

def cleanStrings(string):
    return re.sub("[^a-zA-Z0-9./$:,'&]+", ' ',string) #only include normal string characters
def cleanText(text):
    return re.sub("http[s]?://\S+", ' ', text) #Remove links

commentdf['body'] = commentdf['body'].apply(cleanStrings)
commentdf['body'] = commentdf['body'].apply(cleanText)

classifier = TextClassifier.load('SavedModels/bestModel.pt')

commentdf['sentiment'] = commentdf['sentiment'].astype(object)

for i in range(len(commentdf['body'])):
    if (i % 1000) == 0:
        print(i)
    sent = Sentence(commentdf.loc[i,'body'])
    classifier.predict(sent)
    if len(sent.labels) > 0:
        predictions = sent.labels[0].to_dict()
        commentdf.at[i,'sentiment'] = predictions['value']

uploaddf = commentdf.loc[commentdf['sentiment'] != 0]

bulk_update = db.comments.initialize_unordered_bulk_op()
for index, row in uploaddf.iterrows():
    _id = row['_id']
    stocksArray = row['stocks']
    sentiment = row['sentiment']
    bulk_update.find({'_id': _id}).update_one({'$set':{'stocks': stocksArray, 'sentiment': sentiment}})
bulk_update.execute()
