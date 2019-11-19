from flask import Flask, request
import os
from decouple import config
from flask_cors import CORS, cross_origin
from .timeseries import timeseries


def create_app():
    app = Flask(__name__)
    CORS(app)
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

    @app.route('/')
    @app.route('/index')
    @app.route('/api', methods=['POST'])
    def make_predict():
        data = request.get_json(force=True)
        predict_request = data['business_id']
        result = timeseries(predict_request)
        return result

    return app
