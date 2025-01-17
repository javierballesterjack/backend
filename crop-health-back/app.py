from chalice import Chalice

app = Chalice(app_name='crop-health-back')

@app.route('/health-check', methods=['GET'])
def health_check():
    return {'status': 'ok'}

@app.route('/')
def index():
    return {'hello': 'world'}

@app.route('/test', methods=['GET'])
def lambda_handler(event, context):

    return {'funciona': 'funciona'}


