import json
import psycopg2
from psycopg2.extras import Json
import mgrs  # To convert lat/lon to MGRS

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname='crop-health-db',
    user='master',
    password='JFKae$2341',
    host='crop-health-db.cv0iskeoocuw.eu-north-1.rds.amazonaws.com',
    port='5432'
)
cursor = conn.cursor()

# Load the JSON file
with open('C:\\Users\\Javier\\Desktop\\CROP\\fields_data.json', 'r') as file:
    data = json.load(file)

# Initialize MGRS converter
m = mgrs.MGRS()

# Process each feature
for feature in data['features']:
    client_id = feature['properties']['ID'][:4]  # First 4 characters for client_id
    field_id = feature['properties']['ID'][4:6]  # Next 2 characters for field_id
    crop_type = feature['properties']['CROP']    # Crop type value
    geometry = feature['geometry']               # Polygon coordinates

    # Extract the first coordinate of the polygon
    first_coordinate = geometry['coordinates'][0][0]
    lon, lat = first_coordinate[0], first_coordinate[1]

    # Convert to MGRS
    mgrs_code = m.toMGRS(lat, lon)

    # Extract the Sentinel-2 path components
    utm_zone = mgrs_code[:2]  # First 2 characters (e.g., '30')
    lat_band = mgrs_code[2]   # Third character (e.g., 'T')
    grid_square = mgrs_code[3:5]  # Fourth and fifth characters (e.g., 'TK')

    # Construct Sentinel-2 query path
    sentinel2_query = f"tiles/{utm_zone}/{lat_band}/{grid_square}/"

    # Debugging: Print the values to verify correctness
    print(f"Feature {feature['properties']['ID']} sentinel2_query: {sentinel2_query}")

    try:
        # Insert the data into the fields table
        cursor.execute("""
            INSERT INTO fields (client_id, field_id, crop_type, polygon, sentinel2_query, created_at)
            VALUES (%s, %s, %s, ST_GeomFromGeoJSON(%s), %s, NOW());
        """, (client_id, field_id, crop_type, json.dumps(geometry), sentinel2_query))
        print(f"Inserted feature with ID {feature['properties']['ID']}")
    except Exception as e:
        print(f"Error inserting feature with ID {feature['properties']['ID']}: {e}")

# Commit the transaction
conn.commit()

# Verify insertion
cursor.execute("SELECT * FROM fields LIMIT 10;")
rows = cursor.fetchall()
print("First 10 rows in the fields table:")
for row in rows:
    print(row)

# Close the connection
cursor.close()
conn.close()