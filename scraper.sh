#!/bin/sh
. ~/anaconda3/etc/profile.d/conda.sh
conda activate vsenv
python ~/SentimentTrader/scraper.py
