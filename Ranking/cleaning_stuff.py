# Probably useless code for cleaning business info

def convert_to_json(string):
    result = json.loads(string)
    return result

def get_key(attributes, key):
    result = attributes.get(key)
    return result

# Times are converted to float hours
# 9:30am is 9.5
# 9:30pm is 21.5
def get_time(time):
    hour, minutes = time.split(':')
    hour = int(hour)
    minutes = float(minutes)/60
    time = hour+minutes
    return time

def get_open_close(hours, day):
    try:
        hours = hours.get(day)
        open_time, close_time = hours.split('-')

        open_time = get_time(open_time)
        close_time = get_time(close_time)

    except:
        open_time = np.NaN
        close_time = np.NaN

    finally:
        return (open_time, close_time)


def get_open_time(hours_tuple):
    return hours_tuple[0]
def get_close_time(hours_tuple):
    return hours_tuple[1]

def convert_categories_to_list(categories):
    new_list = []
    try:
        for category in categories.split(','):
            new_list.append(category.strip().replace(' ', '_'))
    except:
        pass
    finally:
        return new_list

def create_categories(cat_list, df):
    for cat in cat_list:
        df[cat] = 1

def clean_business_data(businesses):
    # --------- notes----------#
    # what to do with business_id?
    # filter by is_open?
    # TODO convert attributes to numeric

    #---------- Drop unnecessary columns ----------#
    # filtering all geographic data other than latitude and longitude
    # to avoid double weighting location
    drop_columns = ['address', 'city', 'name', 'postal_code', 'state']
    businesses.drop(drop_columns, axis = 'columns', inplace=True)

    #---------- Get attributes columns ----------#
    # Convert attributes to json
    businesses.attributes = businesses.attributes.apply(convert_to_json)
    # Split attributes to seperate columns
    for key in businesses.attributes.iloc[0]:
        businesses[key] = businesses.attributes.apply(get_key, args=(key,))
    businesses.drop(['attributes'], axis='columns', inplace=True)

    #---------- Get open and close times ----------#
    businesses.hours = businesses.hours.apply(convert_to_json)
    # Creating and filling columns for open and close times each day
    days = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 
        'Thursday', 'Friday', 'Saturday']
    for day in days:
        businesses['temp_hours_tuple'] = businesses.hours.apply(get_open_close, args=(day,))
        businesses[day + 'Open'] = businesses['temp_hours_tuple'].apply(get_open_time)
        businesses[day + 'Close'] = businesses['temp_hours_tuple'].apply(get_close_time)
    
    businesses.drop(['temp_hours_tuple', 'hours'], axis='columns', inplace=True)

    #---------- Getting category columns ----------#

    categories = []
    for business in businesses.categories:
        try:
            for category in business.split(','):
                if category.strip().replace(' ', '_') not in categories:
                    categories.append(category.strip().replace(' ', '_'))
        except:
            pass
    

    #---------- Impute missing values ---------#

    #---------- Encoding ----------#

    # ordinal = NoiseLevel, RestaurantsAttire(None, casual, dressy, formal)
    # categories, BusinessAcceptsCreditCards, RestaurantsPriceRange2, BusinessParking,
    # BikeParking, GoodForKids, RestaruantsTakeOut, RestaurantsGoodForGroups,
    # OutdoorSeating, RestaurantsDelivery, RestaurantsReservations, WiFi, RestaruantsAttire,
    # Alcohol, HasTV, Ambiance, byAppointmentOnly, NoiseLevel, Caters, GoodForMeal,
    # WheelchairAccessible


    return businesses