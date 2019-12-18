import pandas as pd
import category_encoders as ce

#FUNCTION TO GET BUSINESSES IN SAME CATEGORIES AND SAME STATE (OUTPUT TO BE USED IN V1 OF GET COMPETITORS FUNCTION)
# The DF input needs the business_id column to be the index; df = set_index('business_id)
# this function will be used by the next function.
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
# ver2 is included so user can choose which. ver2 does not use knn.

# imports for function
from sklearn.neighbors import NearestNeighbors
from collections import Counter
from scipy.sparse import *
import numpy as np
from scipy import *
import category_encoders as ce

# uses get_category above
def get_competitors(df):
    competitorlist = []
    for i in range(len(df)):
        data = get_categories(df, df.index[i])
        data['common_cat_rank'] = list(range(len(data),0,-1))
        numinfo = data[['is_open', 'latitude', 'longitude', 'review_count', 'stars', 'common_cat_rank']]
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

# #FUNCTION TO GET TOP COMPETITORS FOR EACH BUSINESS - VERSION 2
# #Step 1 - Expand categories column of businesses file
# expand_categories = pd.concat([df, df['categories'].str.split(', ', expand=True)], axis=1)
# expand_categories = expand_categories.rename(columns={0: "category_1", 1: "category_2", 2: "category_3", 3: "category_4", 4: "category_5", 5: "category_6"})
# expand_categories.iloc[:,17:23] = expand_categories.iloc[:,17:23].fillna("No category")
# #Step 2 - Create empty column to be populated with data as function updates records.
# #Pass dataframe through function to get list of top 10 competitors for each business.
# expand_categories['competitors'] = np.nan
# import scipy
# import numpy as np
# from scipy import spatial
# def identify_competitors(df):
#     for i in range(len(df)):
#         if (type(df['competitors'].iloc[i]) != list):
#             categories = df.iloc[i,17:23].values.flatten().tolist()
#             collist = [x for x in categories if not x.startswith('No category')]
#             mask = df.iloc[:,17:23].isin(collist)
#             mask['num_true'] = mask.iloc[:,0:6].sum(axis=1)
#             masktrue = mask.sort_values(by=['num_true'], ascending=False).any(axis=1)
#             filtered = df.loc[masktrue[masktrue].index]
#             num_true = mask.sort_values(by=['num_true'], ascending=False)['num_true'][0:len(filtered)].tolist()
#             filtered['order'] = num_true
#             row = df.iloc[i].to_frame().T
#             for x in df.columns:
#                 row[x]=row[x].astype(row[x].dtypes.name)
#             row['order'] = max(num_true)
#             ary = scipy.spatial.distance.cdist(filtered[['latitude','longitude','percentile', 'order']], \
#                   row[['latitude','longitude','percentile', 'order']], metric='euclidean')
#             top_comp = np.sort(ary.flatten())[:11]
#             mask_comp = np.isin(ary, top_comp)
#             competitors = filtered[mask_comp]['name'].tolist()
#             if len(competitors) > 1:
#                 indeces = filtered[mask_comp].index.tolist()
#                 competitors.pop(0)
#                 info = pd.DataFrame(df[['competitors']][df.index.isin(indeces)])
#                 info['competitors'] = [competitors]*len(info)
#                 df.update(info)
#             else:
#                 pass
#         else:
#             continue
#     return df


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




# run all function is below

# RUN_ALL FUNCTION TO DO PROCESSES
# returns a new DF with all the new data

def run_all(df):
  get_competitors(df)
  get_index(df)
  return df
