"""
Temporary Tokenization Fixer for Reference.

Goal:  Scan tokens in the token column of a dataframe
        Create a subset df with review_id and tokens.
        Save data to S3.
        Generate POST jobs for saved data
"""

import logging
import os

# downloading spacy dependencies
os.system('pip install spacy')
import spacy
os.system('python -m spacy download en_core_web_lg')
nlp = spacy.load('en_core_web_lg') 

from jobs import get_jobs, pop_current_job, read_job,\
     download_data, delete_local_file, delete_s3_file, load_data
from NLP_module import get_df

# downloading and loading spacy models

###Logging###
log_path = os.path.join(os.getcwd(), 'debug.log')
logging.basicConfig(filename=log_path, level=logging.INFO)

### Processing functions ###
def process_text(text):
    doc = nlp(text)

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

    return (tokens, lemmas)

def get_tokens(tuple):
    return tuple[0]

def get_lemmas(tuple):
    return tuple[1]

def filter_tokens(df):
    df['tuple'] = df.text.apply(process_text)
    df['tokens'] = df.tuple.apply(get_tokens)
    df['lemmas'] = df.tuple.apply(get_lemmas)
    df = df.filter(['review_id', 'tokens', 'lemmas']) 
    return df

df = get_df('Clean/clean_review_103')

filtered = filter_tokens(df)
filtered.head()

os.system('echo done')