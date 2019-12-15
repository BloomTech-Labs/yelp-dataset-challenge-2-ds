# Imports for function

import pandas as pd
import collections, functools, operator
import numpy as np
import operator


# the sentiment chunks function, who's values are strings
def get_sentiment_chunks(df):
    top_chunks = []
    top_sentiment = []
    ids = df['business_id'].unique().tolist()
    for i in range(len(ids)):
        filtered = df[df['business_id'] == ids[i]]
        dicts = [ {df['noun_chunks'].iloc[i][j] : df['polarity'].iloc[i] } for j in range(len(df['noun_chunks'].iloc[i])) ]
        result = dict(functools.reduce(operator.add, map(collections.Counter, dicts)))
        sorted_x = sorted(result.items(), key=operator.itemgetter(1))
        top_10 = sorted(sorted_x, key=lambda t: t[1], reverse=True)[:10]
        last_10 = sorted(sorted_x, key=lambda t: t[1], reverse=True)[-10:]
        together = top_10 + last_10
        chunks = [i[1] for i in together]
        sentiment = [i[1] for i in together]
        for j in range(len(filtered)):
            top_chunks.append(chunks)
            top_sentiment.append(sentiment)
    df['top_chunks'] = top_chunks
    df['top_sentiment'] = top_sentiment
    return df