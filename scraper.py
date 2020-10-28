import pymongo
import pandas as pd
import praw
import time
import datetime
import re
import nltk

from datetime import datetime, timedelta
from pymongo import MongoClient
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

def getContinuousChunks(text):
    chunked = nltk.ne_chunk(nltk.pos_tag(nltk.word_tokenize(text)))
    continuousChunkdf = pd.DataFrame(columns = ['id', 'Named Entity', 'Label'])
    currentChunk = []
    currentLabel = []
    for i in chunked:
        if type(i) == nltk.tree.Tree:
            currentChunk.append(" ".join([token for token, pos in i.leaves()]))
            currentLabel.append(i.label())
        if currentChunk:
            namedEntity = " ".join(currentChunk)
            label = " ".join(currentLabel)
            if namedEntity not in continuousChunkdf['Named Entity']:
                d = {'id': 0, 'Named Entity': namedEntity, 'Label': label}
                continuousChunkdf = continuousChunkdf.append(d, ignore_index = True)
                currentChunk = []
                currentLabel = []
        else:
            continue
    return continuousChunkdf

def mapResults(result, leftNames, rightNames, threadID, threshold):
    result[result < threshold] = 0
    matchdf = pd.DataFrame(0, index = np.arange(len(result.nonzero()[0])), columns = ['id','Left', 'Right', 'Similarity'])
    for i in range(len(result.nonzero()[0])):
        matchdf.loc[i, 'Left'] = leftNames[result.nonzero()[0][i]]
        matchdf.loc[i, 'Right'] = rightNames[result.nonzero()[1][i]]
        matchdf.loc[i, 'Similarity'] = result[result.nonzero()[0][i]][result.nonzero()[1][i]]
    matchdf['id'] = threadID
    return matchdf.drop_duplicates(subset = 'Right')

def pullTickers(string):
    dollarTicker = set(re.findall(r"\$\b[A-Z]{1,4}\b",string))
    manualTicker = re.findall(r"\b[A-Z]{2,4}\b",string)
    manualTicker = list(set(manualTicker).difference(notTickers))
    manualTicker = set(['$' + manualTicker for manualTicker in manualTicker])
    combined = manualTicker.union(dollarTicker)
    return combined.intersection(companydf['Manual'])

notTickers = {'DCF', 'IMO', 'CAN', 'MMS', 'ARE', 'CDC', 'NEW', 'LOVE', 'NYC', 'CASH', 'AI', 
'NAV', 'GOOD', 'DD', 'ATH', 'APPS', 'EDIT', 'WOW', 'PCB', 'UNIT', 'TA', 'VG', 'SELF', 'MR',
'RARE', 'ALEX', 'KEY', 'STIM', 'GO', 'SEE', 'CFO', 'CAL', 'JOE', 'REV', 'PE', 'CHI', 'EVE', 
'CO', 'EV', 'TTM', 'EOD', 'AT', 'HUGE', 'ES', 'ONE', 'PT', 'CEO', 'ZEN', 'NOW', 'JAN', 'O',
'OR', 'PG', 'ROCK', 'FOUR', 'ONE', 'TWO', 'FIVE', 'SIX', 'NINE', 'TEN', 'ON', 'SU', 'XT',
'WELL', 'NOV', 'MAR', 'JAN', 'FUN', 'NOW', 'VERY'}

wantedTags = ['DD', 'Technicals', 'Fundamentals', 'Discussion', 'YOLO', 'Stocks']

client = MongoClient('localhost', 27017)
db = client.db
threads = db.threads
companies = db.companylist
secret = db.secret.find_one()['Secret']

reddit = praw.Reddit(client_id='-wpcPIbA7bhlpw', client_secret=secret, user_agent='sentiment')
subreddit = reddit.subreddit('wallstreetbets')
postlist = []

lastInsertedDate = db.threads.find_one(sort = [('_id',pymongo.DESCENDING)])['Inserted Date']

companydf = pd.DataFrame.from_records(companies.find())

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
postDf['Stocks'] = postDf['Stocks'].astype(object)

def cleanStrings(string):
    return re.sub("[^a-zA-Z0-9./$:,']+", ' ',string)

postDf['Title'] = postDf['Title'].apply(cleanStrings)
postDf['Body'] = postDf['Body'].apply(cleanStrings)

companyNames = companydf['Name'].unique()
companySymbols = companydf['Symbol'].unique()
nameVectorizer = TfidfVectorizer(min_df = 1)
symbolVectorizer = TfidfVectorizer(min_df = 1)
companyMatrix = nameVectorizer.fit_transform(companyNames)
symbolMatrix = symbolVectorizer.fit_transform(companySymbols)

for i in range(len(postDf['Body'])):
    sent = postDf['Title'][i] + '. ' + postDf['Body'][i]
    threadID = i
    chunks = getContinuousChunks(sent)
    chunkdf = chunks.loc[(chunks['Label'] == 'ORGANIZATION') | (chunks['Label'] == 'PERSON')].reset_index()
    stocklist = []

    if len(chunkdf['Label']) > 0:
        nerNameMatrix = nameVectorizer.transform(chunkdf['Named Entity'])
        nerSymbolMatrix = symbolVectorizer.transform(chunkdf['Named Entity'])
        nameResult = cosine_similarity(nerNameMatrix, companyMatrix)
        symbolResult = cosine_similarity(nerSymbolMatrix, symbolMatrix)
        namedf = mapResults(nameResult, chunkdf['Named Entity'], companyNames, threadID, 0.85)
        symboldf = mapResults(symbolResult, chunkdf['Named Entity'], companySymbols, threadID, 1)
        stocklist = list(set(namedf.loc[namedf['Similarity'] > 0.8, 'Right'].append(symboldf.loc[symboldf['Similarity'] > 0.999, 'Right'])))
    postDf.at[threadID, 'Stocks'] = stocklist
postDf = postDf.drop(postDf.loc[postDf['Stocks'].str.len() == 0].index).reset_index()

threads.insert_many(postDf.to_dict('records'))