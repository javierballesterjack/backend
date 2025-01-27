from chalice import Chalice
from chalicelib.my_packages import insert_fields
from chalicelib.my_packages import plot_current_fields
from chalicelib.my_packages import plot_health_metrics

app = Chalice(app_name='crop-health-back')

@app.route('/health-check', methods=['GET'])
def health_check():
    return {'status': 'ok'}

@app.route('/')
def index():
    return {'hello': 'world'}

@app.route('/insert_fields', methods=['POST'],cors=True)
def insert_fields_route():
    request = app.current_request
    return insert_fields.insert_fields(request)

@app.route('/plot_current_fields', methods=['POST'],cors=True)
def plot_current_fields_route():
    request = app.current_request
    return plot_current_fields.plot_current_fields(request)

@app.route('/plot_health_metrics', methods=['POST'],cors=True)
def plot_health_metrics_route():
    request = app.current_request
    return plot_health_metrics.plot_health_metrics(request)
