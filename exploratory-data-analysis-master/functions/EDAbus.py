# dfs for testing
import pandas as pd
example = pd.read_csv('aggregatedata.csv')
df = pd.read_json('sample_business.json')

#FUNCTION TO GET BUSINESSES IN SAME CATEGORIES AND SAME STATE (OUTPUT TO BE USED IN V1 OF GET COMPETITORS FUNCTION)
# used business_id from the first row
bus_id = '4B6HGu5C68dfUk5_N2lbYg'
def get_categories(df, bus_id):
    filter = df.loc[bus_id]
    categories = filter['categories'].split(",")
    cleans = [s.strip() for s in categories]
    numcommon = []
    for i in range(len(df)):
        if (df['categories'].iloc[i] != None and df['state'].iloc[i] == df['state'].loc[bus_id]):
            rows_text = df['categories'].iloc[i].split(",")
            rowsclean = [s.strip() for s in rows_text]
            incommon = set(cleans) & set(rowsclean)
            noitems = len(incommon)
            if noitems > 0:
                for j in range(noitems):
                    numcommon.append(df.index[i])
    mostcommon = [item for items, c in Counter(numcommon).most_common() for item in [items] * c]
    seen = set()
    finalist = [x for x in mostcommon if not (x in seen or seen.add(x))]
    final_df = df.loc[finalist]
    return df


#FUNCTION TO GET TOP COMPETITORS FOR EACH BUSINESS - VERSION 1 (USING KNN)
from sklearn.neighbors import NearestNeighbors
from collections import Counter
from scipy.sparse import *
import numpy as np
from scipy import *
def get_competitors(df):
    competitorlist = []
    for i in range(len(df)):
        data = get_categories(df, df.index[i])
        data['common_cat_rank'] = list(range(len(data),0,-1))
        numinfo = data[['is_open', 'latitude', 'longitude', 'num_categories', 'review_count', 'stars', 'postal_code', 'common_cat_rank']]
        numcomp = len(numinfo)
        if numcomp < 11:
            n_neighbors = numcomp
        else:
            n_neighbors = 11
        bus_matrix = csr_matrix(numinfo.values)
        knn_comp = NearestNeighbors(metric = 'cosine', algorithm = 'brute')
        knn_comp.fit(bus_matrix)
        distances, indices = knn_comp.kneighbors(numinfo.loc[df.index[i]].values.reshape(1, -1), n_neighbors = n_neighbors)
        competitors = [numinfo.index[indices.flatten()[j]] for j in range(1, len(distances.flatten()))]
        competitorlist.append(competitors)
    df['competitors'] = competitorlist
    return df


#FUNCTION TO GET PERCENTILE SCORE FOR EACH BUSINESS
#Step 1 - Create empty columns in businesses dataframe to be populated with score and percentile
df['percentile'] = np.nan
df['score'] = np.nan
#Step 2 - Run function to calculate ranking and percentile
def get_index(df):
    for i in range(len(df)):
        if (pd.isnull(df['percentile'].iloc[i]) == True and df['categories'].iloc[i] != None):
            data = get_categories(df, df.index[i])
            final_scores = []
            for i in range(len(data)):
                review_score = (data['review_count'].iloc[i]/data['review_count'].max())*100
                star_score = (data['stars'].iloc[i]/5)*100
                total_score = (star_score*.50) + (review_score*0.50)
                final_scores.append(total_score)
            if len(final_scores) > 1:
                data['score'] = final_scores
                sz = data['score'].size-1
                data['percentile'] = data['score'].rank(method='max').apply(lambda x: 100.0*(x-1)/sz)
                data['best_sector'] = [data.nlargest(5, 'percentile').index.tolist()]*len(data)
                df.update(data)
            else:
                pass
        else:
            continue
    return df


#FUNCTION TO GET AVERAGE STAR RATING OVER AVAILABLE TIME PERIOD FROM BUSINESSES FILE
def avg_star_rating(df):
   df['date_list'] = df.apply(lambda row: [row['date_time']], axis=1)
   df['star_review_list'] = df.apply(lambda row: [row['star_review']], axis=1)
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


# run all function is below

# RUN_ALL FUNCTION TO DO PROCESSES

def run_all(df):
  get_categories(df, bus_id)
  get_competitors(df)
  get_index(df)
  avg_star_rating(df)
  return df
