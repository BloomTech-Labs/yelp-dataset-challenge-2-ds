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

<<<<<<< HEAD
    result = reviews.query.with_entities(reviews.token, reviews.date, \
             reviews.stars).filter_by(business_id=bus_id)
    df = pd.read_sql(sql = result.statement, con = DB.engine)
    df['token'] = df['token'].apply(lambda x: [i.strip('{').strip('}') for i in x.split(',')])
    print(df['token'].iloc[0])
    filtered = df.sort_values('date')
    filtered = filtered.reset_index()
=======
    # Group processed response
>>>>>>> b8a3bdbdef699df85ddad9fa0341da952f8e4d1c
    filtered['bins'] = pd.qcut(filtered.index, q=10, precision=0)
    new_df = filtered.groupby('bins').agg({'token': 'sum', \
            'stars': 'mean', 'date': lambda x: x.iloc[-1]})

<<<<<<< HEAD
    counts = []
    for i in range(len(new_df)):
        wc_df = wc_count(new_df['token'].values[i])
        wc_df['date'] = new_df['date'].values[i]
        wc_df['star_review'] = new_df['stars'].values[i]
        counts.append(wc_df)

    df_final = pd.concat(counts)
    df_final['date'] = df_final['date'].astype(str)
    df_final = df_final.reset_index()
    output = (df_final.groupby(['date'], as_index=True)
             .apply(lambda x: x[['word','count','pct_total','rank',\
                 'star_review']].to_dict('r'))
              .to_json()).replace("'", "")

    return output
=======
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
>>>>>>> b8a3bdbdef699df85ddad9fa0341da952f8e4d1c
