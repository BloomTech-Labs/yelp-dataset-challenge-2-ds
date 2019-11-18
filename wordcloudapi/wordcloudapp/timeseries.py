import numpy as np
import pandas as pd
import requests
import json
import ast
from collections import Counter


def wc_count(docs):
    """Count the occurance of each word and rank
    """
    total=len(docs)
    wc = pd.DataFrame({'word':docs, 'count':np.ones(len(docs))})
    wc = wc.groupby('word').sum()
    wc['pct_total'] = wc['count']/total
    wc['rank'] = wc['count'].rank(method='first', ascending=False)
    return wc.sort_values(by='rank').nlargest(30, 'count')


def timeseries(bus_id):
    # Query database API for data
    df = get_reviews(bus_id)

    filtered = df.sort_values('date')
    filtered = filtered.reset_index()
    filtered['bins'] = pd.qcut(filtered.index, q=10, precision=0)
    new_df = filtered.groupby('bins').agg({'tokens': 'sum', \
            'star_review': 'mean', 'date': lambda x: x.iloc[-1]})

    counts = []
    for i in range(len(new_df)):
        wc_df = wc_count(new_df['tokens'].values[i])
        wc_df['date'] = new_df['date'].values[i]
        wc_df['star_review'] = new_df['star_review'].values[i]
        counts.append(wc_df)

    df_final = pd.concat(counts)
    df_final['date'] = df_final['date'].astype(str)
    df_final = df_final.reset_index()
    output = (df_final.groupby(['date'], as_index=True)
             .apply(lambda x: x[['word','count','pct_total','rank',\
                 'star_review']].to_dict('r'))
              .to_json()).replace("'", "")

    return output


def get_reviews(business_id, url='https://db-api-yelp18-staging.herokuapp.com/api/data'):
    """Create JSON request object and send GET request to database api
    """
    package = {
    'schema': 'biz_words',
    'params': {
        'business_id': business_id
        },
    }
    response = requests.get(url=url, json=package)
    return strip_tokens_badchar(
        pd.DataFrame(json.loads(response.text)['data'], columns=['data', 'tokens', 'star_review'])
    )

# Custom Cleaning function for current token information.
# Remove for performance gain in future data release
def strip_tokens_badchar(dataframe):
    # Clean string with simple regex
    dataframe['tokens'] = dataframe['tokens'].str.strip('\{').str.strip('\}').\
        str.strip('"\n\n,"').str.strip('"\n"').str.split(',')

    # Drop items of short length
    def length_min(x):
        return [word for word in x if len(word)>3]

    dataframe.tokens = dataframe.tokens.apply(length_min)
    return dataframe