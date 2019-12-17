#FUNCTION TO GET TOP 10 POSITIVE AND NEGATIVE NOUN CHUNKS
#Step 1 - Get reviews file and group by business id and aggregate noun chunks column and polarity score column:
sentiment_chunks = df.groupby('business_id').agg({'ngram': 'sum', 'polarity_list': 'sum'})
#Step 2 - Create empty columns to be populated with list of noun chunks and sentiment scores.
#Process noun chunks to return list of top 10 positive and negative noun chunks for each business (one column for sentiment and second column for list of noun chunks)
sentiment_chunks['top_chunks'] = np.nan
sentiment_chunks['top_sentiment'] = np.nan
from collections import defaultdict
def get_sentiment_chunks(df):
    for i in range(len(df)):
        if (type(df['top_chunks'].iloc[i]) != list):
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

# RUN_ALL function for ease of use

def run_all(df):
  get_sentiment_chunks(df)
  reviews_by_year(df)
  get_review_dist(df)
  return df