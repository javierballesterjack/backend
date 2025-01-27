import psycopg2
from datetime import datetime
from psycopg2.extras import RealDictCursor
import json

def default_serializer(obj):
    """
    Custom serializer for handling non-serializable objects like datetime.
    """
    if isinstance(obj, datetime):
        return obj.isoformat()  # Convert datetime to ISO 8601 string
    raise TypeError(f"Type {type(obj)} not serializable")

def plot_health_metrics(event):
    try:
        # Obtener el JSON enviado en la solicitud
        data = event.json_body

        field_id = data.get('field_id')
        username = data.get('username')

        conn = psycopg2.connect(
            dbname='crop-health-db',
            user='master',
            password='tO5YZSVTs52OVfrP5H92',
            host='crop-health-db.cv0iskeoocuw.eu-north-1.rds.amazonaws.com',
            port='5432'
        ) 
        
        if not username or not field_id:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Missing required query parameters: username and field"})
            }
        
        query = """
        SELECT 
            date,
            ndwi, 
            ndvi, 
            savi, 
            evi, 
            crop_type 
        FROM crop_health
        WHERE username = %s
        AND field_id = %s;
        """
        
        # Execute query with parameters
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (username, field_id))
            rows = cursor.fetchall()
        conn.close()
        
        return {
            "statusCode": 200,
            "body": json.dumps(rows, default=default_serializer)  # Use custom serializer
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }

