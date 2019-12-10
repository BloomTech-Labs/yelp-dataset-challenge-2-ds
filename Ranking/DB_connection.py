from config import DATABASE_URI
from models import * # download models.py from db_api folder
from db import get_session # download db.py from db_api folder

def biz_words(session, params, *args, **kwargs):
    response = session.query(Review.date, Review.token, Review.stars).filter(Review.business_id==params['business_id']).order_by(Review.date)
    return {'data': response.all()}

params = {'business_id' : '1SWheh84yJXfytovILXOAQ'}

with get_session() as session:
    test = biz_words(session, params)
    print(test)

