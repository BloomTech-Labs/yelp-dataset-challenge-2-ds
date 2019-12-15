import ast
from flask import jsonify
from .models import DB, reviews, Viz2, Business


def create_dict(list1, list2, key1, key2):
    """Create list of dictionaries from two lists
    """
    list_dict = []
    for i in range(len(list1)):
        dictionary = {key1 : list1[i], key2 : list2[i]}
        list_dict.append(dictionary)
    return list_dict


def clean_data(result):
    """Clean data and coerce into necessary data structure
    """
    result[0]['competitors'] = ast.literal_eval(result[0]['competitors'].\
                               strip('"').strip("'").replace("\\\\",'').\
                                   replace("\\",''))
    result[0]['bestinsector'] = ast.literal_eval(result[0]['bestinsector'].\
                                strip('"').strip("'").\
                                replace("\\\\",'').replace("\\",''))
    starsovertime = ast.literal_eval(result[0]['avg_stars_over_time'].\
                    strip("'").strip('"').replace("\\\\",'').\
                    replace("\\",''))
    sentimentchunks = ast.literal_eval(result[0]['chunk_sentiment'].\
                      strip("'").strip('"').replace("\\\\",'').\
                      replace("\\",''))
    result[0]['avg_stars_over_time'] = create_dict(starsovertime[1], \
                                      starsovertime[0], 'date', 'stars')
    result[0]['chunk_sentiment'] = create_dict(sentimentchunks[0], \
                                   sentimentchunks[1], 'chunks', 'sentiment')
    countbystar = ast.literal_eval(result[0]['count_by_star'].strip('"'))
    stars, starcounts = list(countbystar.keys()), list(countbystar.values())
    result[0]['count_by_star'] = create_dict(stars, starcounts, \
                                'stars', 'count')
    reviewbyyear = ast.literal_eval(result[0]['review_by_year'].strip('"'))
    years, reviews = list(reviewbyyear.keys()), list(reviewbyyear.values())
    result[0]['review_by_year'] = create_dict(years, reviews, 'year', 'total')
    return result
    

def jsondata(bus_id):
    """Query database, clean data and output final JSON response
    """
    q1 = Viz2.query.filter_by(business_id=bus_id).all()
    q2 = Business.query.filter_by(business_id=bus_id).all()
    q1s = [{**i.serialize, **j.serialize} for i,j in zip(q1,q2)]
    cleaned = clean_data(q1s)
    data = jsonify(data=cleaned)

    return data