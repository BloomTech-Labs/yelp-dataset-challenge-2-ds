import pandas as pd
import time
import logging
import os

# installing / importing textblob and spacy
os.system('pip install textblob')
os.system('pip install spacy')
from textblob import TextBlob
import spacy

###Logging###
logger = logging.getLogger(__name__+" NLP Processing")

# using the spacy library to vectorize existing tokens
# these download models
# python -m spacy download en_vectors_web_lg
# python -m spacy download en_core_web_lg
# python -m spacy validate

nlp = spacy.load("en_core_web_lg")

# Spacy functions
def process_doc(doc):
    # Defining parts of speech to keep in tokens and lemmas
    POS = ['ADJ', 'NOUN', 'PROPN', 'VERB', 'ADV', 'INTJ']

    # Getting lemmas and tokens
    lemmas = []
    tokens = []
    for token in doc:
        if ((token.is_stop != True)
        and (token.is_punct != True)
        and (token.pos_ in POS)):
            tokens.append(token.text)
            lemmas.append(token.lemma_)

    # Getting noun_chunks
    noun_chunks = [span.text for span in doc.noun_chunks]

    # Getting vectors
    vectors = doc.vector

    return (tokens, lemmas, noun_chunks, vectors)

def get_tokens(tuple):
    return tuple[0]

def get_lemmas(tuple):
    return tuple[1]

def get_noun_chunks(tuple):
    return tuple[2]

def get_vectors(tuple):
    return tuple[3]

# Sentiment functions

def process_text(text):
    blob = TextBlob(text)

    sentiment = blob.sentiment
    """sentiment is a tuple
        
        sentiment[0] = polarity 
        range: -1 (most negative) to 1 (most positive)

        sentiment[1] = objectivity
        range: 0 (most subjective) to 1 (most objective)
    """
    return sentiment

def get_polarity(tuple):
    return tuple[0]

def get_subjectivity(tuple):
    return tuple[1]

def process_df(df):
    start_main = time.time()
    df['doc'] = list(nlp.pipe(df.text.to_numpy(), batch_size=100))
    df['tuple'] = df.doc.apply(process_doc)
    df['token'] = df.tuple.apply(get_tokens)
    df['lemma'] = df.tuple.apply(get_lemmas)
    df['noun_chunk'] = df.tuple.apply(get_noun_chunks)
    df['token_vector'] = df.tuple.apply(get_vectors)
    df['sentiment'] = df.text.apply(process_text)
    df['polarity'] = df.sentiment.apply(get_polarity)
    df['subjectivity'] = df.sentiment.apply(get_subjectivity)
    df.drop(['doc', 'tuple', 'sentiment'], axis='columns', inplace=True)

    stop_main = time.time()
    logger.info('Batch of {} processed in {}'.format(len(df), stop_main-start_main))

    return df