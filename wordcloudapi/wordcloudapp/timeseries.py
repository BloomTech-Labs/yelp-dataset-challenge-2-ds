import numpy as np
import pandas as pd
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

    result = reviews.query.with_entities(reviews.token, reviews.date, \
             reviews.stars).filter_by(business_id=bus_id)
    df = pd.read_sql(sql = result.statement, con = DB.engine)
    df['token'] = df['token'].apply(lambda x: [i.strip('{').strip('}') for i in x.split(',')])
    print(df['token'].iloc[0])
    filtered = df.sort_values('date')
    filtered = filtered.reset_index()
    filtered['bins'] = pd.qcut(filtered.index, q=10, precision=0)
    new_df = filtered.groupby('bins').agg({'token': 'sum', \
            'stars': 'mean', 'date': lambda x: x.iloc[-1]})

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