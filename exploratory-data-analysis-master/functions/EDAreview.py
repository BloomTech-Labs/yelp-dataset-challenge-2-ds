import pandas as pd
import numpy as np
import operator

#FUNCTION TO GET TOP 10 POSITIVE AND NEGATIVE NOUN CHUNKS
#Step 1 - Get reviews file and group by business id and aggregate noun chunks column and polarity score column:
df['polarity'] = sentiment['polarity']
df['ngram'] = df['ngram'].apply(lambda x: x.tolist())
df['polarity_list'] = df.apply(lambda row: [row['polarity']]*len(row['ngram']), axis=1)
sentiment_chunks = df.groupby('business_id').agg({'ngram': 'sum', 'polarity_list': 'sum'})

#Step 2 - Create empty columns to be populated with list of noun chunks and sentiment scores.
#Process noun chunks to return list of top 10 positive and negative noun chunks for each business (one column for sentiment and second column for list of noun chunks)

sentiment_chunks['top_chunks'] = np.nan
sentiment_chunks['top_sentiment'] = np.nan
from collections import defaultdict
def get_sentiment_chunks(df):
    for i in range(len(df)):
        if (type(sentiment_chunks['top_chunks'].iloc[i]) != list):
            dicts = [ {df['ngram'].iloc[i][j] : df['polarity_list'].iloc[i][j] } for j in range(len(df['ngram'].iloc[i])) ]
            result = {}
            intermediate = defaultdict(list)
            for subdict in dicts:
                for key, value in subdict.items():
                    intermediate[key].append(value)
            for key, value in intermediate.items():
                result[key] = sum(value)/len(value)
            sorted_x = sorted(result.items(), key=operator.itemgetter(1))
            top_10 = sorted(sorted_x, key=lambda t: t[1], reverse=True)[:10]
            last_10 = sorted(sorted_x, key=lambda t: t[1], reverse=True)[-10:]
            together = top_10 + last_10
            chunks = [i[0] for i in together]
            sentiment = [i[1] for i in together]
            info = pd.DataFrame([[chunks, sentiment]], index=[i], columns=['top_chunks','top_sentiment'])
            df.update(info)
        else:
            pass
    return df

#FUNCTION TO GET AVERAGE STAR RATING OVER AVAILABLE TIME PERIOD FROM REVIEWS FILE
from datetime import datetime
def avg_star_rating(df):
   df['date_time'] = pd.to_datetime(df['date'])
   df['date_list'] = df.apply(lambda row: [row['date_time']], axis=1)
   df['star_review_list'] = df.apply(lambda row: [row['stars']], axis=1)
   grouped = df.sort_values(by=['business_id', 'date_time'], ascending=True).groupby('business_id')\
                               .agg({'date_list': 'sum', 'star_review_list': 'sum'})
   grouped['star_review_list'] = grouped['star_review_list'].apply(lambda x: np.array(x))
   grouped['date_list'] = grouped['date_list'].apply(lambda x: np.array(x, dtype='datetime64[s]'))
   grouped['star_ave'] = grouped['star_review_list'].apply(lambda x: [x[np.digitize(list(range(0, len(x), 1)),\
                         np.linspace(0, len(x), 11)) == i].mean() for i in range(1, \
                         len(np.linspace(0, len(x), 11)))] if len(x) > 10 else x)
   grouped['date_ave'] = grouped['date_list'].apply(lambda x: [x[np.digitize(list(range(0, len(x), 1)), \
                         np.linspace(0, len(x), 11)) == i].view('i8').mean().astype('datetime64[s]') \
                         for i in range(1, len(np.linspace(0, len(x), 11)))] if len(x) > 10 else x)
   return grouped


#FUNCTION FOR GETTING REVIEWS BY YEAR
def reviews_by_year(df):
   reviewbyyear = df.groupby(['business_id', df['date_time'].dt.year.rename('year')]).agg({'business_id': 'count'}).unstack()
   reviewbyyear.columns = reviewbyyear.columns.map(lambda x: ''.join([*map(str, x)]))
   reviewbyyear = reviewbyyear.fillna(0)
   return reviewbyyear
#FUNCTION FOR GETTING DISTRIBUTION OF STARS FOR EACH BUSINESS
def get_review_dist(df):
   review_dist = df.groupby(['business_id', 'star_review']).agg({'star_review': 'count'}).unstack()
   review_dist.columns = review_dist.columns.map(lambda x: ''.join([*map(str, x)]))
   review_dist = review_dist.reset_index()
   return review_dist



