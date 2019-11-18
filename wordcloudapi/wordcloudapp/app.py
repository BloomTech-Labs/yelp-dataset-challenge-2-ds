from flask import Flask, jsonify, request
import os
import requests
from decouple import config
from flask_cors import CORS, cross_origin
from .timeseries import timeseries


def create_app():
    app = Flask(__name__)
    CORS(app)
    app.config['SQLALCHEMY_DATABASE_URI'] = config('DATABASE_URL')
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    DB.init_app(app)

    @app.route('/')
    @app.route('/index')
    @app.route('/api', methods=['POST'])
    def make_predict():
        data = request.get_json(force=True)
        predict_request = data['business_id']
        result = timeseries(predict_request)
        return result

    return app
