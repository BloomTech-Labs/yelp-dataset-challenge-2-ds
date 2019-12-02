"""
Dropin functions and examples for GET Requests

"""

# Biz Words
def get_reviews(business_id, url='https://db-api-yelp18-staging.herokuapp.com/api/data'):
    import requests
    package = {
    'schema': 'biz_words',
    'params': {
        'business_id': business_id
        },
}
    response = requests.get(url=url, json=package)
    ### Add Response Formatting Here ###
    return response.text