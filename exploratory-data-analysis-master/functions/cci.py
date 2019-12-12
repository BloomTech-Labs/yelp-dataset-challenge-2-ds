# Imports for functions

from sklearn.neighbors import NearestNeighbors
from collections import Counter
from scipy.sparse import *
import numpy as np
from scipy import *

# input the df you want to run function on and it's bus_id will be a string
# get_categories requires the business_id to be the index for the inputs. Running it without gets key errors.
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
    return final_df

# the get_competitors function requires the get_categories function to be working
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
    #             df = df.merge(data[['percentile', 'score']], left_index=True, right_on='percentile', how='left')
                data['best_sector'] = [data.nlargest(5, 'percentile').index.tolist()]*len(data)
                df.update(data)
            else:
                pass
        else:
            continue
    return df



