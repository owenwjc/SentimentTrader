import flair
from flair.models import TextClassifier
from flair.data import Sentence
from flair.datasets import ClassificationCorpus
from flair.embeddings import WordEmbeddings, FlairEmbeddings, DocumentRNNEmbeddings
from flair.trainers import ModelTrainer
from pathlib import Path

def main():

    corpus = ClassificationCorpus(Path('data/'), test_file='test.csv', dev_file='dev.csv', train_file='train.csv')
    checkpoint = 'model/checkpoint.pt'
    trainer = ModelTrainer.load_checkpoint(checkpoint, corpus)
    trainer.train('model/',
                  max_epochs=100,
                  embeddings_storage_mode='none',
                  patience=5,
                  checkpoint=True)
    
if __name__ == "__main__":
    main()