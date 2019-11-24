import numpy as np
import pandas as pd
import requests
import ujson
import ast
from collections import Counter
from .models import DB, reviews

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
    # Deprecated - See db_api get request functionality
    # result = reviews.query.with_entities(reviews.tokens, reviews.date, \
    #          reviews.star_review).filter_by(business_id=bus_id)
    # df = pd.read_sql(sql = result.statement, con = DB.engine)
    # df['tokens'] = df['tokens'].str.strip('\[').str.strip('\]').\
    #     str.split(', ')
    # filtered = df.sort_values('date')

    filtered = get_reviews(business_id=bus_id).reset_index()
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
        pd.DataFrame(ujson.loads(response.text)['data'], columns=['date', 'tokens', 'star_review'])
    )

# Custom Cleaning function for current token information.
# Remove for performance gain in future data release
def strip_tokens_badchar(dataframe):
    # Clean string with simple regex
    dataframe['tokens'] = dataframe['tokens'].str.strip('\{').str.strip('\}').\
        str.split(',')

    return dataframe
