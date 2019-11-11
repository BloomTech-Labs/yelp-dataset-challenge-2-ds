import requests

test_data = {
    'table_name': 'businesses',
    'data': [
            {
        "businessid": 'pioharegh04q3ur0qha089h23r2q3oiqhbef09q1234h',
        "name": 'Big Biz Inc',
        "latitude": 1.001,
        "longitude": 1.002,
        "postalcode": 1234,
        "numreviews": 9,
        "stars": 3.4,
        "isopen": 0,
        "attributes": 'some number of attributes, maybe a comma',
        "categories": 'some number of categories, maybe a comma',
        },
        {
        "businessid": 'pioharadfq342ha089h23r2q3oiqhbef09q1234h',
        "name": 'Big Biz Competitor Inc',
        "latitude": 1.004,
        "longitude": 1.006,
        "postalcode": 9999,
        "numreviews": 2,
        "stars": 3.8,
        "isopen": 1,
        "attributes": 'some number of attributes, maybe a comma',
        "categories": 'some number of categories, maybe a comma',
        }

    ]
}

## Build post request
request = requests.post(url='http://localhost:5000/api/data/', json=test_data)
print(request)