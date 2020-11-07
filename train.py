import pymongo
import pandas as pd
import flair
from flair.models import TextClassifier
from flair.data import Sentence
from flair.datasets import ClassificationCorpus
from flair.embeddings import WordEmbeddings, FlairEmbeddings, DocumentRNNEmbeddings
from flair.trainers import ModelTrainer
from pathlib import Path

client = pymongo.MongoClient('localhost',27017)
db = client.db
comments = db.comments

df = pd.DataFrame.from_records(comments.find({'Label':{'$in': ['bullish', 'bearish']}}))

labelDf = pd.DataFrame(columns = ['Label', 'Text'])
labelDf['Text'] = df['Body']
labelDf['Label'] = df['Label']
labelDf['Label'] = '__label__' + labelDf['Label'].astype(str)

labelDf = labelDf.sample(frac = 1)
labelDf.iloc[0: int(len(labelDf)*0.8)].to_csv('data/train.csv', sep = '\t', index = False, header = False)
labelDf.iloc[int(len(labelDf)*0.8): int(len(labelDf)*0.9)].to_csv('data/test.csv', sep = '\t', index = False, header = False)
labelDf.iloc[int(len(labelDf)*0.9): ].to_csv('data/dev.csv', sep = '\t', index = False, header = False)

corpus = ClassificationCorpus(Path('data/'), test_file='test.csv', dev_file='dev.csv', train_file='train.csv')
word_embeddings = [WordEmbeddings('glove'), FlairEmbeddings('news-forward-fast'), FlairEmbeddings('news-backward-fast')]
document_embeddings = DocumentRNNEmbeddings(word_embeddings, hidden_size=512, reproject_words=True, reproject_words_dimension=256)
classifier = TextClassifier(document_embeddings, label_dictionary=corpus.make_label_dictionary(), multi_label=False)
trainer = ModelTrainer(classifier, corpus)
trainer.train('model/', max_epochs=100, embeddings_storage_mode='none', learning_rate=0.2, patience=5, checkpoint=True)