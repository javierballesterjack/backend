from chalice import Chalice
import psycopg2
from datetime import datetime
import json
from chalicelib.my_packages import insert_fields
app = Chalice(app_name='crop-health-back')

@app.route('/health-check', methods=['GET'])
def health_check():
    return {'status': 'ok'}

@app.route('/')
def index():
    return {'hello': 'world'}

@app.route('/insert_fields', methods=['POST'],cors=True)
def lambda_handler():
    request = app.current_request
    return insert_fields.insert_fields(request)
