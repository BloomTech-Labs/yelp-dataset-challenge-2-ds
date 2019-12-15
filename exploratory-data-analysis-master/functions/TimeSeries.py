# Imports for functions

from collections import Counter
import pandas as pd
import numpy as np
import ast
import datetime as datetime

# This function will let us use timeseries in our features
def timeseries(bus_id):

    result = reviews.query.filter_by(business_id=bus_id)
    df = pd.read_sql(sql = result.statement, con = DB.engine)
    filtered = df.sort_values('date')
    no_reviews = len(filtered)
    filtered['tokens'] = filtered['tokens'].apply(ast.literal_eval)
    x = no_reviews//10
    output = {}
    dictionaries = []
    def listoflists(lst):
        return [[el] for el in lst]
    if no_reviews >= 20:
        agg_tokens = np.add.reduceat(filtered['tokens'].values, \
                                     np.arange(0, len(filtered['tokens']), x))
        nth = filtered.iloc[::x, :]
        date = nth['date'].values
        avg_stars = np.add.reduceat(filtered['star_review'].values, \
                    np.arange(0, len(filtered['star_review']), x))/ \
                    np.bincount(np.resize(np.repeat(np.arange(0,(len(filtered['star_review'])//x)+1), x), \
                    len(filtered['star_review'])))
        new_df = pd.DataFrame({'tokens':agg_tokens, 'date':date, 'avg_stars': avg_stars})
    else:
        agg_tokens = filtered['tokens'].values
        date = filtered['date'].values
        avg_stars = filtered['star_review'].values
        new_df = pd.DataFrame({'tokens':agg_tokens, 'date':date, 'avg_stars': avg_stars})
    for i, ddate in enumerate(new_df['date']):
        if len(set(new_df['tokens'].values[i])) < 30:
            for j in range(len(set(new_df['tokens'].values[i]))):
                extract = listoflists(new_df['tokens'].values[i])
                wc = count(extract)
                wc_final = wc[wc['rank'] <= 30]
                row_dict = dict({"word": str(wc_final['word'].values[j]), \
                                 "count": str(wc_final['count'].values[j]), \
                                 "rank": str(wc_final['rank'].values[j]), \
                                 "avg_stars": str(new_df['avg_stars'].values[i]), \
                                 "pct_total": str(wc_final['pct_total'].values[j])})
                dictionaries.append(row_dict)
        else:
            for j in range(30):
                extract = listoflists(new_df['tokens'].values[i])
                wc = count(extract)
                wc_final = wc[wc['rank'] <= 30]
                row_dict = dict({"word": str(wc_final['word'].values[j]), \
                                 "count": str(wc_final['count'].values[j]), \
                                 "rank": str(wc_final['rank'].values[j]), \
                                 "avg_stars": str(new_df['avg_stars'].values[i]), \
                                 "pct_total": str(wc_final['pct_total'].values[j])})
                dictionaries.append(row_dict)
        review_date = dict({str(ddate): dictionaries})
        output.update(review_date)
        dictionaries = []
    return output
