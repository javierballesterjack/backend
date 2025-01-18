from chalice import Chalice
import psycopg2
from datetime import datetime
import json

app = Chalice(app_name='crop-health-back')

@app.route('/health-check', methods=['GET'])
def health_check():
    return {'status': 'ok'}

@app.route('/')
def index():
    return {'hello': 'world'}

@app.route('/test', methods=['POST'],cors=True)
def lambda_handler():
    try:
        # Obtener el JSON enviado en la solicitud
        request = app.current_request
        data = request.json_body

        # Extraer los datos necesarios del JSON
        email = data.get('email')
        username = data.get('username')
        polygons = data.get('polygons')
        sentinel_queries = data.get('sentinel_queries')
        timestamp = datetime.now()

        if not email or not username or not polygons or not sentinel_queries:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': 'Faltan datos obligatorios en el JSON enviado.',
                    'required_fields': ['email', 'username', 'polygons', 'sentinel_queries']
                })
            }

        # Conectar a la base de datos PostgreSQL
        conn = psycopg2.connect(
            dbname='crop-health-db',
            user='master',
            password=,
            host='crop-health-db.cv0iskeoocuw.eu-north-1.rds.amazonaws.com',
            port='5432'
        )
        cursor = conn.cursor()

        # Asegurarse de que polygons es una lista y procesar cada uno
        if isinstance(polygons, list):
            # Obtener el máximo field_id para el usuario
            cursor.execute("SELECT MAX(field_id) FROM trial WHERE username = %s", (username,))
            result = cursor.fetchone()
            max_field_id = result[0] if result[0] is not None else 0

            # Insertar cada polígono con un field_id único
            for i, polygon in enumerate(polygons):
                field_id = max_field_id + i + 1
                sentinel_query = sentinel_queries[i]
                field_name = "Field"
                crop_type = "Crop"

                # Convertir las coordenadas del polígono al formato requerido
                polygon_str = "POLYGON((" + ", ".join([f"{coord[1]} {coord[0]}" for coord in polygon]) + "))"

                insert_query = """
                    INSERT INTO trial (username, field_id, field_name, crop_type, polygon, created_at, sentinel2_query)
                    VALUES (%s, %s, %s, %s, ST_GeomFromText(%s, 4326), %s, %s)
                """
                cursor.execute(insert_query, (username, field_id, field_name, crop_type, polygon_str, timestamp, sentinel_query))
        else:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': 'Polygons data is not a list.',
                    'polygons_type': str(type(polygons))
                })
            }

        # Confirmar los cambios en la base de datos
        conn.commit()

        # Cerrar la conexión
        cursor.close()
        conn.close()

        # Respuesta de éxito
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Polygons successfully inserted.'})
        }

    except Exception as e:
        # Manejo de errores
        print(f"Error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Error inserting data.', 'error': str(e)})
        }
