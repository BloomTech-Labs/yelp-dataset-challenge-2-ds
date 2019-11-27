# NLP notebooks, files, and docs

Overview of NLP modules for text preprocessing, analysis, and related job creation.  NLP preprocessing is done in advance of analysis wherever possible to reduce client-side latency and on-demand compute requirements.

## Preprocessing

### MODULE: NLP_module.py

Primary control module for NLP preprocessing and job creation.  Invokes the following on clean datasets:

> NLP_processing.py
> TODO: NLP_sentiment.py (Currently temp_sentiment_analysis.py in development)

### MODULE: NLP_processing.py

Tokenization, n_grams and/or noun_chunks, lemmatization, and vectorization of documents.

### MODULE: NLP_sentiment.py

In Development - See Sentiemnt Analysis Below

## Tokenization & Vectorization

Tokenization and filtering of stop words is our first step in preprocessing.  At that time, Spacy provides 300 element word vectors which are averaged from each doc and reported.  Lemmas of passing tokens are also provided as well as noun_chunks from the UNFILTERED document, giving ngrams that may contain stopwords, but give better context to their phrase.

### Spacy Usage

**V1.0**

> Model: en_core_web_lg used for tokenization AND vectorization.

### Example Output

## Sentiment Analysis

Description

### TextBlob Usage

### Example Output

The temp_sentiment_analysis file will give a dataframe with the following columns in this order:

1. 'review_id' or 'tip_id'

2. 'polarity'

3. 'subjectivity'
