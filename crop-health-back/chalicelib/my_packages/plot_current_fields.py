import psycopg2
from datetime import datetime
from psycopg2.extras import RealDictCursor
import json

def plot_current_fields(event):
    try:
        # Obtener el JSON enviado en la solicitud
        data = event.json_body

        # Extraer los datos necesarios del JSON
        username = data.get('username')

        if not username:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Missing username query parameter'})
            }

        # Conectar a la base de datos PostgreSQL
        conn = psycopg2.connect(
            dbname='crop-health-db',
            user='master',
            password='tO5YZSVTs52OVfrP5H92',
            host='crop-health-db.cv0iskeoocuw.eu-north-1.rds.amazonaws.com',
            port='5432'
        ) 
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # SQL query to fetch fields and convert polygon to GeoJSON
        select_query = """
        SELECT 
            username, 
            field_id,
            field_name, 
            crop_type, 
            ST_AsGeoJSON(polygon) AS polygon 
        FROM fields
        WHERE username = %s
        """
        cursor.execute(select_query, (username,))

        # Fetch all rows
        rows = cursor.fetchall()

        # Convert datetime objects to strings
        for row in rows:
            for key, value in row.items():
                if isinstance(value, datetime):
                    row[key] = value.isoformat()  # Convert datetime to ISO 8601 string format

        # Close the connection
        cursor.close()
        conn.close()

        # Return a success response with the rows
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Data successfully fetched', 'data': rows})
        }

    except Exception as e:
        # Return the error along with the query parameters in the response body
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Error fetching data',
                'error': str(e)
            })
        }