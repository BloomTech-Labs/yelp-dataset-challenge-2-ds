import numpy as np
import pandas as pd
import requests
import ujson
# import ast
# from collections import Counter
# from .models import DB, reviews

def wc_count(agg_row):
    """Count the occurance of each word and rank
    """
    docs = agg_row[0]
    date = agg_row[1]
    star_review = agg_row[2]
    total=len(docs)
    wc = pd.DataFrame({'word':docs, 'count':np.ones(total)})
    wc = wc.groupby('word').sum()
    wc['pct_total'] = wc['count']/total
    wc['rank'] = wc['count'].rank(method='first', ascending=False)
    wc['date'] = date
    wc['star_review'] = star_review
    return wc.sort_values(by='rank').nlargest(30, 'count')


def timeseries(bus_id):
    # Deprecated - See db_api get request functionality
    # result = reviews.query.with_entities(reviews.tokens, reviews.date, \
    #          reviews.star_review).filter_by(business_id=bus_id)
    # df = pd.read_sql(sql = result.statement, con = DB.engine)
    # df['tokens'] = df['tokens'].str.strip('\[').str.strip('\]').\
    #     str.split(', ')
    # filtered = df.sort_values('date')

    # Send API request and process response to DataFrame
    filtered = get_reviews(business_id=bus_id)

    # Group processed response
    filtered['bins'] = pd.qcut(filtered.index, q=10, precision=0)
    new_df = filtered.groupby('bins').agg({'tokens': 'sum', \
            'star_review': 'mean', 'date': lambda x: x.iloc[-1]})

    # Get word counts
    counts = list(map(wc_count, new_df.to_numpy()))

    # Generate output <- This is causing the problem.  The dates end up out of order for some reason
    df_final = pd.concat(counts)
    df_final['date'] = df_final['date'].astype(str)
    df_final = df_final.reset_index()
    output = (df_final.groupby(['date'], as_index=True)
             .apply(lambda x: x[['word','count','pct_total','rank',\
                 'star_review']].to_dict('r'))
              .to_json()).replace("'", "")

    return counts


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
