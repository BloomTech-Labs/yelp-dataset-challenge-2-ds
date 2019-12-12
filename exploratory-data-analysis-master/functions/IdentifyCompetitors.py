# imports for function

import scipy
import numpy as np
from scipy import spatial


def identify_competitors(df):
    for i in range(len(df)):
        if (type(df['competitors'].iloc[i]) != list):
            categories = df.iloc[i,17:23].values.flatten().tolist()
            collist = [x for x in categories if not x.startswith('No category')]
            mask = df.iloc[:,17:23].isin(collist)
            mask['num_true'] = mask.iloc[:,0:6].sum(axis=1)
            masktrue = mask.sort_values(by=['num_true'], ascending=False).any(axis=1)
            filtered = df.loc[masktrue[masktrue].index]
            num_true = mask.sort_values(by=['num_true'], ascending=False)['num_true'][0:len(filtered)].tolist()
            filtered['order'] = num_true
            row = df.iloc[i].to_frame().T
            for x in df.columns:
                row[x]=row[x].astype(row[x].dtypes.name)
            row['order'] = max(num_true)
            ary = scipy.spatial.distance.cdist(filtered[['latitude','longitude','percentile', 'order']], row[['latitude','longitude','percentile', 'order']], metric='euclidean')
            top_comp = np.sort(ary.flatten())[:11]
            mask_comp = np.isin(ary, top_comp)
            competitors = filtered[mask_comp]['name'].tolist()
            if len(competitors) > 1:
                indeces = filtered[mask_comp].index.tolist()
                competitors.pop(0)
#                 info = pd.DataFrame([[competitors]], index=[df.index[i]], columns=['competitors'])
                info = pd.DataFrame(df[['competitors']][df.index.isin(indeces)])
                info['competitors'] = [competitors]*len(info)
                df.update(info)
            else:
                pass
        else:
            continue
    return info