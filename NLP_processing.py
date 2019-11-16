import spacy
import pandas as pd
import time
import logging

###Logging###
logger = logging.getLogger(__name__+" NLP Processing")

# using the spacy library to vectorize existing tokens
# these download models
# python -m spacy download en_vectors_web_lg
# python -m spacy download en_core_web_lg
# python -m spacy validate

nlp = spacy.load("en_core_web_lg")

def process_text(text):
    doc = nlp(text)
 
    # Getting lemmas and tokens
    lemmas = []
    tokens = []
    for token in doc:
        if (token.is_stop != True) and (token.is_punct != True):
            tokens.append(token.text)
            lemmas.append(token.lemma_)
    
    # Getting noun_chunks
    noun_chunks = list(doc.noun_chunks)
    
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

def process_df(df):
    start_main = time.time()

    df['tuple'] = df.text.apply(process_text)
    df['tokens'] = df.tuple.apply(get_tokens)
    df['lemmas'] = df.tuple.apply(get_lemmas)
    df['noun_chunks'] = df.tuple.apply(get_noun_chunks)
    df['vectors'] = df.tuple.apply(get_vectors)
    df.drop(['tuple'], axis='columns', inplace=True)

    stop_main = time.time()
    logger.info('Batch of {} processed in {}'.format(len(df), stop_main-start_main))
    
    return df