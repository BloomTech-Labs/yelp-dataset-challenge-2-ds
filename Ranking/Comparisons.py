### Comparison module ###
# Module for adding nearest_neighbor business IDs and distances to DB

import pandas as pd
import logging
import os
import json
from pandas.io.json import json_normalize
from config import DATABASE_URI
from models import *
from db import get_session
from sqlalchemy import or_
from jobs import get_jobs, pop_current_job, read_job, \
    download_data, delete_local_file, delete_s3_file, load_data, \
        write_data, generate_job
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import NearestNeighbors
from sqlalchemy.exc import SQLAlchemyError
from tqdm import tqdm

class cache_item:
    def __init__(self, state, categories, filtered_df=None, model=None):
        self.state = state
        self.categories = categories
        self.filtered_df = filtered_df
        self.model = model
    

# cache for storing models and queries
# example entry: 'CA-category1-category2-category3' : model
cache = []

binary = {'False':0, 'True':1, 'None':0}
conversion_dict = {'AcceptsInsurance':binary,
    'Alcohol':{'none':0, 'None':0, 'beer_and_wine':1, 'full_bar':2}, 
    'BusinessAcceptsCreditCards':binary,
    'ByAppointmentOnly':binary,
    'Caters':binary,
    'CoatCheck':binary, 
    'Corkage':binary, 
    'DogsAllowed':binary,
    'DriveThru':binary, 
    'GoodForDancing':binary, 
    'GoodForKids':binary,
    'HappyHour':binary, 
    'HasTV':binary, 
    'NoiseLevel':{'quiet':0, 'average':1, 'loud':2, 'very_loud':3, 'None':1},
    'OutdoorSeating':binary, 
    'RestaurantsAttire':{'casual':0, 'dressy':1, 'formal':2, 'None':0},
    'RestaurantsDelivery':binary,
    'RestaurantsGoodForGroups':binary, 
    'RestaurantsPriceRange2':{'1':1, '2':2, '3':3, '4':4, '5':5, 'None':2},
    'RestaurantsReservations':binary, 
    'RestaurantsTableService':binary,
    'RestaurantsTakeOut':binary, 
    'Smoking':{'no':0, 'outdoor':1, 'yes':2, 'None':0}, 
    'WheelchairAccessible':binary, 
    'WiFi':{'no':0, 'paid':1, 'free':2, 'None':2}
}


###---------Processing Functions----------###
def get_business_df(business_id):
    try:
        with get_session() as session:

            # Getting info about business for filtering
            business_query = session.query(Business.business_id,
                Business.state, Business.latitude, Business.longitude, \
                Business.attributes, Business.categories 
                ).filter(Business.business_id==business_id).all()
            business_df = pd.DataFrame(business_query)
            business_df = clean_business_data(business_df)

    # handling database errors, recursively calling get_business_df()
    except SQLAlchemyError as e:
        main_logger.exception(str(e.__dict__['orig']))
        business_df = get_business_df(business_id)

    finally:
        return business_df


def get_filtered_df(business_df):

    state = business_df.iloc[0].state
    categories = business_df.iloc[0].categories

    # check if filtered_df is cached already
    for item in cache:
        if item.state == state and \
            item.categories == categories and  \
            item.filtered_df !:
            print('returning item from cache')
            return item.filtered_df

    try:
        with get_session() as session:
            filter_query = session.query(Business.business_id, \
                Business.latitude, Business.longitude, Business.attributes, \
                Business.categories).filter(Business.state==state).filter(or_( \
                *[Business.categories.contains(category) for category in \
                categories])).all()
            filtered_df = pd.DataFrame(filter_query)
            filtered_df = clean_business_data(filtered_df)
    
    # handling database errors, recursively calling get_business_df()
    except SQLAlchemyError as e:
        main_logger.exception(str(e.__dict__['orig']))
        filtered_df = get_filtered_df(business_df)
    
    finally:
        item = cache_item(state, categories, filtered_df=filtered_df)
        cache.append(item)
        return filtered_df

def get_categories(categories):
    categories = set(categories.split(', '))
    return categories

def clean_business_data(df):

    # convert attributes from string to dict
    df['attributes'] = df.attributes.apply(json.loads)

    # create new column for each attribute
    attributes = json_normalize(df['attributes'])

    # combining attributes and original dataframes
    cleaned = pd.concat([df, attributes], axis=1)
    cleaned.drop(['attributes'], axis='columns', inplace=True)

    # drop unnecessary attribute columns
    drop_columns = ['Ambience', 'AgesAllowed', 'BYOB', 'BYOBCorkage', 'BestNights', 'BikeParking', \
    'BusinessAcceptsBitcoin', 'BusinessParking', 'DietaryRestrictions', \
    'GoodForMeal', 'HairSpecializesIn', 'Music', 'Open24Hours', \
    'RestaurantsCounterService']
    cleaned.drop(drop_columns, axis='columns', inplace=True)

    # converting categories from string to set
    cleaned.categories = df.categories.apply(get_categories)

    # setting index to business_id
    cleaned.index = cleaned.business_id
    cleaned.drop(['business_id'], axis='columns', inplace=True)

    return cleaned

def extra_categories(categories, business_categories):
    n_common_categories = len(categories & business_categories)
    extra_categories = len(categories) - n_common_categories
    return extra_categories

def category_check(categories, category):
    if category in categories:
        return 1
    else:
        return 0

def get_comparison_group(business_df, filtered_df):

    state = business_df.state.iloc[0]

    # creating new column for each category in original business
    business_categories = business_df.categories.iloc[0]
    for category in business_categories:
        filtered_df[category] = filtered_df.categories.apply(category_check, args=[category,])
        business_df[category] = 1
    
    # creating new column with count of categories 
    # the original business didn't have
    business_df['extra_categories'] = 0
    filtered_df['extra_categories'] = filtered_df.categories.apply( \
        extra_categories, args=[business_categories,])

    # dropping categories columns
    business_df.drop(['categories'], axis='columns', inplace=True)
    filtered_df.drop(['categories'], axis='columns', inplace=True)
    
    # dropping state column in business_df
    business_df.drop(['state'], axis='columns', inplace=True)

    # Imputing missing values in business_df with value of 
    # None in conversion dict
    for column in business_df.columns:
        if business_df[column].isna().iloc[0]:
            lookup_dict = conversion_dict[column]
            business_df[column].iloc[0] = lookup_dict['None']

    # imputing missing values
    imputer = SimpleImputer(strategy="most_frequent")
    imputed = pd.DataFrame(imputer.fit_transform(filtered_df))
    imputed.columns = business_df.columns
    
    # encode columns
    encoded = encode_attributes(imputed)
    encoded_business = encode_attributes(business_df)

    # scaling data
    scaler = StandardScaler()
    scaled = scaler.fit_transform(encoded)
    scaled = pd.DataFrame(scaled, columns = business_df.columns)
    
    # manual encoding should convert almost all attribute entries to numbers
    # if not, they will be converted to NaN after scaling
    scaled = scaled.fillna(0)

    scaled_business = scaler.transform(encoded_business)
    scaled_business = pd.DataFrame(scaled_business, columns = scaled.columns)

    # get cached model, model is initialized to None 
    # in cache so this will return None or actual model
    for item in cache:
        if item.state == state and item.categories == business_categories:
            neigh = item.model
    
    # if model not created, create it and add to cache
    if neigh == None:
        neigh = NearestNeighbors(n_neighbors=26)
        neigh.fit(scaled)
        for item in cache:
            if item.state == state and item.categories == business_categories:
                item.model = neigh

    # business must be in same form as df
    __distances, indices = neigh.kneighbors(scaled_business)

    # getting string list of competitor ids
    comps = ''
    for i in range(1,26):
        business_id = filtered_df.index[indices[0][i]]
        delimiter = ', '
        comps += business_id + delimiter
    
    # slice off unused last delimiter
    comps = comps[:-2]

    return comps

def convert(attribute, lookup_dict):
    return lookup_dict.get(attribute)

def encode_attributes(df):

    # converting data in df based on conversion_dict
    for column in df.columns:
        if column in conversion_dict:
            lookup_dict = conversion_dict.get(column)
            df[column] = df[column].apply(convert, args=[lookup_dict,])
    return df

def get_comps(business_id):
    business_df = get_business_df(business_id)
    # if business categories is not None, get comps
    if business_df.categories.iloc[0]:
        filtered_df = get_filtered_df(business_df)
        comps = get_comparison_group(business_df, filtered_df)
    # otherwise, return none
    else: 
        comps = None
    return comps

def get_comp_df():
    try:
        with get_session() as session:

            # Getting all business_ids in db with competitors
            # TODO check if competitors already filled out
            id_query = session.query(Business.business_id, Viz2.competitors\
                ).join(Viz2).all()
            comp_df = pd.DataFrame(id_query)
    except:
        comp_df = get_comp_df()

    return comp_df

if __name__ == "__main__":

    ###-------------Logging-------------------###
    main_logger = logging.getLogger(__name__+" Comparison Controller")
    log_path = os.path.join(os.getcwd(), 'debug.log')
    logging.basicConfig(filename=log_path, level=logging.INFO)

    ###----Getting list of all business_ids----###
    comp_df = get_comp_df()

    tqdm.pandas()
    comp_df['competitor_ids'] = comp_df.business_id.progress_apply(get_comps)

    # TODO here down

    # Write Data to s3
    savepath = asset+'_comparisons'
    write_data(data=sentiment_df, savepath=savepath, dry_run=False)
    
    # Generate POST Job
    review = asset.split('_')[1] == 'review'
    if review:
        generate_job(savepath, 'POST', tablename='review_sentiment', dry_run=False)
    else:
        generate_job(savepath, 'POST', tablename='tip_sentiment', dry_run=False)

    # Cleanup
    delete_local_file(datapath)
    delete_s3_file(current_job)
    main_logger.info("Deleted Job: {}".format(current_job))
    # read job
    # get appropriate file
    # for each business_id:
    #   businesses = filtered_data(business_id)
    #   comps = get_comparison_group
    pass

