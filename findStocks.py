import pandas as pd
import yfinance as yf
import pymongo
import time
import datetime
import re
import numpy as np
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
    dollarTicker = list(set(re.findall(r"\$\b[A-Z]{1,4}\b",string)))
    manualTicker = re.findall(r"\b[A-Z]{2,4}\b",string)
    manualTicker = set(manualTicker).difference(notTickers)
    manualTicker = set(['$' + manualTicker for manualTicker in manualTicker])
    combined = manualTicker.union(dollarTicker)
    combined = list(combined.intersection(companydf['Manual']))
    combined = set([combined[1:] for combined in combined])
    return combined

notTickers = {'DCF', 'IMO', 'CAN', 'MMS', 'ARE', 'CDC', 'NEW', 'LOVE', 'NYC', 'CASH', 'AI', 
'NAV', 'GOOD', 'DD', 'ATH', 'APPS', 'EDIT', 'WOW', 'PCB', 'UNIT', 'TA', 'VG', 'SELF', 'MR',
'RARE', 'ALEX', 'KEY', 'STIM', 'GO', 'SEE', 'CFO', 'CAL', 'REV', 'PE', 'CHI', 'EVE', 'PDT',
'CO', 'EV', 'TTM', 'EOD', 'AT', 'HUGE', 'ES', 'ONE', 'PT', 'CEO', 'ZEN', 'NOW', 'JAN', 'O',
'OR', 'PG', 'ROCK', 'FOUR', 'ONE', 'TWO', 'FIVE', 'SIX', 'NINE', 'TEN', 'ON', 'SU', 'XT',
'WELL', 'NOV', 'MAR', 'JAN', 'FUN', 'NOW', 'VERY', 'USA', 'POST', 'ALL', 'IT', 'GDP', 'RH',
}

client = MongoClient('localhost', 27017)
db = client.db
comments = db.comments
companies = db.companylist

companydf = pd.DataFrame.from_records(companies.find())
commentdf = pd.DataFrame.from_records(comments.find({'$and': [{'stocks': {'$exists': False}},{'created_utc': {'$exists': True}}]}))

commentdf['sentiment'] = 0
commentdf['stocks'] = 0
commentdf['stocks'] = commentdf['stocks'].astype(object)

def cleanStrings(string):
    return re.sub("[^a-zA-Z0-9./$:,'&]+", ' ',string) #only include normal string characters
def cleanText(text):
    return re.sub("http[s]?://\S+", ' ', text) #Remove links

commentdf['body'] = commentdf['body'].apply(cleanStrings)
commentdf['body'] = commentdf['body'].apply(cleanText)

companyNames = companydf['Name'].unique()
companySymbols = companydf['Symbol'].unique()
nameVectorizer = TfidfVectorizer(min_df = 1)
symbolVectorizer = TfidfVectorizer(min_df = 1)
companyMatrix = nameVectorizer.fit_transform(companyNames)
symbolMatrix = symbolVectorizer.fit_transform(companySymbols)

for i in range(len(commentdf)):
    print(i)
    sent = commentdf['body'][i]
    threadID = i
    chunks = getContinuousChunks(sent)
    chunkdf = chunks.loc[(chunks['Label'] == 'ORGANIZATION') | (chunks['Label'] == 'PERSON')].reset_index()
    stocklist = set()

    if len(chunkdf['Label']) > 0:
        nerNameMatrix = nameVectorizer.transform(chunkdf['Named Entity'])
        nerSymbolMatrix = symbolVectorizer.transform(chunkdf['Named Entity'])
        nameResult = cosine_similarity(nerNameMatrix, companyMatrix)
        symbolResult = cosine_similarity(nerSymbolMatrix, symbolMatrix)
        namedf = mapResults(nameResult, chunkdf['Named Entity'], companyNames, threadID, 0.85)
        symboldf = mapResults(symbolResult, chunkdf['Named Entity'], companySymbols, threadID, 1)
        stocklist = set(namedf.loc[namedf['Similarity'] > 0.8, 'Right'].append(symboldf.loc[symboldf['Similarity'] > 0.999, 'Right']))
    commentdf.at[threadID, 'stocks'] = stocklist

commentdf['stocks'] = [set(a).union(b) for a,b in zip(commentdf['stocks'], commentdf['body'].apply(pullTickers))]

for i in range(len(commentdf['stocks'])):
    tickerset = commentdf.loc[i, 'stocks']
    named = tickerset.intersection(companydf['Name'])
    if len(named) > 0:
        tickerset = tickerset.difference(named)
        namedTickers = set()
        for j in named:
            namedTickers.add(companydf.loc[companydf['Name'] == j, 'Symbol'].iloc[0])
        commentdf.at[i,'stocks'] = tickerset.union(namedTickers)
    
commentdf['stocks'] = [list(tickers) for tickers in commentdf['stocks']]

bulk_update = db.comments.initialize_unordered_bulk_op()
for index, row in commentdf.iterrows():
    _id = row['_id']
    stocksArray = row['stocks']
    sentiment = row['sentiment']
    bulk_update.find({'_id': _id}).update_one({'$set':{'stocks': stocksArray, 'sentiment': sentiment}})
bulk_update.execute()