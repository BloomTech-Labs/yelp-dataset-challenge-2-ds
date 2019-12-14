import numpy as np
import pandas as pd
import requests
import ujson

def wc_count(agg_row):
    """Count the occurance of each word and rank
    """
    docs = agg_row[0]
    total=len(docs)
    wc = pd.DataFrame({'word':docs, 'count':np.ones(total)})
    wc = wc.groupby('word').sum()
    wc['pct_total'] = wc['count']/total
    wc['rank'] = wc['count'].rank(method='first', ascending=False)
    wc['date'] = agg_row[2]
    wc['star_review'] = agg_row[1]
    wc['word'] = wc.index
    print(wc)
    return wc.sort_values(by='rank').nlargest(30, 'count')


def timeseries(bus_id):
    # Request data from API
    filtered = get_reviews(business_id=bus_id)

    # Group processed response
    filtered['bins'] = pd.qcut(filtered.index, q=10, precision=0)
    new_df = filtered.groupby('bins').agg({'tokens': 'sum', \
            'star_review': 'mean', 'date': lambda x: x.iloc[-1]})

    # Get word counts
    counts = list(map(wc_count, new_df.to_numpy()))

    # Generate Output BETA 2
    output = {}
    for group in counts:
        date = group.date[0]
        package = group.drop(columns='date').to_dict('r')
        output[date] = package

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