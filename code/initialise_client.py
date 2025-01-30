import pyproj
import rasterio
import numpy as np
from shapely.geometry import Polygon, Point
from rasterio.windows import from_bounds
from rasterio.windows import transform as window_transform
import psycopg2
from psycopg2.extras import DictCursor
import boto3
import os
import psycopg2
from datetime import datetime, timedelta
import json
import shutil

def lonlat_to_utm(lon, lat, zone_number):
    """Convert longitude and latitude to UTM coordinates."""
    wgs84_proj = pyproj.CRS("EPSG:4326")  # WGS84 (longitude, latitude)
    utm_proj = pyproj.CRS(f"EPSG:326{zone_number}")  # UTM Zone 30T

    transformer = pyproj.Transformer.from_crs(wgs84_proj, utm_proj, always_xy=True)
    return transformer.transform(lon, lat)

def convert_polygon_to_utm(polygon, zone_number):
    """Convert a list of polygon points from lon/lat to UTM (meters)."""
    return [lonlat_to_utm(lon, lat, zone_number) for lon, lat in polygon]

def get_points_in_polygon(polygon, x_vals, y_vals, ndwi_values, ndvi_values, savi_values, evi_values):
    """Get the values of grid points inside the polygon."""
    polygon_shape = Polygon(polygon)
    ndwi_list = []
    ndvi_list = []
    savi_list = []
    evi_list = []
    # Generate a 2D grid of coordinates
    grid_x, grid_y = np.meshgrid(x_vals, y_vals)
    grid_x_flat = grid_x.flatten()
    grid_y_flat = grid_y.flatten()

    # Ensure values are flattened to match the grid
    ndwi_values_flat = ndwi_values.flatten()
    ndvi_values_flat = ndvi_values.flatten()
    savi_values_flat = savi_values.flatten()
    evi_values_flat = evi_values.flatten()
    
    for x, y, ndwi_val, ndvi_val, savi_val, evi_val in zip(grid_x_flat, grid_y_flat, ndwi_values_flat, ndvi_values_flat, savi_values_flat, evi_values_flat):
        point = Point(x, y)
        if polygon_shape.contains(point):
            ndwi_list.append(ndwi_val)
            ndvi_list.append(ndvi_val)
            savi_list.append(savi_val)
            evi_list.append(evi_val)
        
    return ndwi_list, ndvi_list, savi_list, evi_list

def calculate_average_indicators_for_polygon(polygon, tci_image_path, nir_image_path, zone_number):
    """Calculate average NDWI, NDVI, SAVI, and EVI for the given polygon."""
    
    # Open the TCI image and NIR image
    with rasterio.open(tci_image_path) as src_tci, rasterio.open(nir_image_path) as src_nir:

        # Convert polygon to UTM and calculate the bounding box
        polygon_utm = convert_polygon_to_utm(polygon, zone_number)
        all_x = [coord[0] for coord in polygon_utm]
        all_y = [coord[1] for coord in polygon_utm]
        min_x, max_x = min(all_x), max(all_x)
        min_y, max_y = min(all_y), max(all_y)

        # Read a subset of the image using the bounding box
        polygon_window = from_bounds(min_x, min_y, max_x, max_y, src_tci.transform)
        subset_rgb_image = src_tci.read([1, 2, 3], window=polygon_window)
        subset_nir_image = src_nir.read(1, window=polygon_window)  # Read NIR band for subset
        subset_ndwi_image = (subset_rgb_image[1, :, :] - subset_nir_image) / (subset_rgb_image[1, :, :] + subset_nir_image)
        subset_ndvi_image = (subset_nir_image - subset_rgb_image[0, :, :]) / (subset_nir_image + subset_rgb_image[0, :, :])
        
        # Calculate SAVI (Soil-Adjusted Vegetation Index)
        savi_image = (subset_nir_image - subset_rgb_image[0, :, :]) / (subset_nir_image + subset_rgb_image[0, :, :] + 0.5)  # Using L=0.5
        
        # Calculate EVI (Enhanced Vegetation Index)
        evi_image = 2.5 * (subset_nir_image - subset_rgb_image[0, :, :]) / (subset_nir_image + 6 * subset_rgb_image[1, :, :] - 7.5 * subset_rgb_image[2, :, :] + 1)
        
        # Get the transform for the subset
        subset_transform = window_transform(polygon_window, src_tci.transform)

        # Generate 1D arrays for the x and y coordinates
        cols = subset_ndwi_image.shape[1]  # Number of columns
        rows = subset_ndwi_image.shape[0]  # Number of rows

        x_vals = np.arange(cols) * subset_transform[0] + subset_transform[2] + 5  # X coordinates
        y_vals = np.arange(rows) * subset_transform[4] + subset_transform[5] - 5  # Y coordinates

        # Get points inside the polygon
        ndwi_list, ndvi_list, savi_list, evi_list = get_points_in_polygon(
            polygon_utm, x_vals, y_vals, subset_ndwi_image, subset_ndvi_image, savi_image, evi_image
        )

        # Calculate the average NDWI, NDVI, SAVI, and EVI for the polygon
        if ndwi_list:
            average_ndwi = np.mean(ndwi_list)
            average_ndvi = np.mean(ndvi_list)
            average_savi = np.mean(savi_list)
            average_evi = np.mean(evi_list)
            return average_ndwi, average_ndvi, average_savi, average_evi, len(ndwi_list)
        else:
            return None

# S3 Configuration
BUCKET_NAME = "sentinel-s2-l2a"
LOCAL_DIRECTORY = r"C:\Users\Javier\Desktop\CROP\Sentinel2_trial"

# Create the directory if it doesn't exist
os.makedirs(LOCAL_DIRECTORY, exist_ok=True)

# PostgreSQL Connection
conn = psycopg2.connect(
    dbname='crop-health-db',
    user='master',
    password='tO5YZSVTs52OVfrP5H92',
    host='crop-health-db.cv0iskeoocuw.eu-north-1.rds.amazonaws.com',
    port='5432'
)
cursor = conn.cursor(cursor_factory=DictCursor)

# Step 1: Retrieve the rows for a specific username
username = "b0dc99ec-3001-70da-3960-a47eda1623fe"  # Replace this with the actual username you are filtering by

cursor.execute("""
    SELECT 
        *, 
        ST_AsGeoJSON(polygon) AS polygon_geojson 
    FROM fields 
    WHERE username = %s;
""", (username,))

results = cursor.fetchall()  # Fetch all rows

if results:
    # Find the oldest created_at date
    oldest_date = None
    sentinel2_query = None

    for row in results:
        created_at = row[5]  # Assuming created_at is the 6th column
        if oldest_date is None or created_at < oldest_date:
            oldest_date = created_at
            sentinel2_query = row[6]  # Assuming sentinel2_query is the 7th column

    print(f"Oldest created_at: {oldest_date}")
    
    # Step 2: Calculate the starting_date (one year before oldest_date)
    starting_date = oldest_date - timedelta(days=230)
    print(f"Starting date (one year before): {starting_date}")

    # Format the starting date for file path (e.g., 'YYYY/M/D')
    starting_date_str = f"{starting_date.year}/{starting_date.month}/{starting_date.day}"

    print(f"Sentinel-2 query: {sentinel2_query}")

    # Step 4: Initialize S3 client and download files
    s3_client = boto3.client("s3")

    # Step 5: Iterate over each day between starting_date and today
    current_date = starting_date
    while current_date <= datetime.today():
        # Construct file paths using the current_date
        current_date_str = f"{current_date.year}/{current_date.month}/{current_date.day}"
        FILES_TO_DOWNLOAD = [
            f"{sentinel2_query}{current_date_str}/0/R10m/TCI.jp2",
            f"{sentinel2_query}{current_date_str}/0/R10m/B08.jp2",
            f"{sentinel2_query}{current_date_str}/0/qi/CLD_20m.jp2",
            
        ]
        
        file_paths = []
        success = True
        count = 0
        invalid = False
        for file in FILES_TO_DOWNLOAD:
            if invalid == False:
                try:
                    # Construct full local file path
                    local_file_folder = os.path.join(LOCAL_DIRECTORY, current_date_str)
                    local_file_path = os.path.join(local_file_folder, file.split("/")[-1])
                    file_paths.append(local_file_path)
                    os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

                    # Download file from S3
                    s3_client.download_file(BUCKET_NAME, file, local_file_path)
                    if count == 0:
                        with rasterio.open(local_file_path) as nir:
                            nir = nir.read(1) 
                            if np.sum(nir==0)>12056040 or np.sum(nir>=255)>108504360:
                                invalid = True
                                print("Invalid data, skipping to the next day")
                                shutil.rmtree(local_file_folder)
                    if not invalid:
                        print(f"Successfully downloaded {file} to {local_file_path}")
    
                except Exception as e:
                    print(f"Failed to download {file}: {str(e)}")
                    success = False
                    shutil.rmtree(local_file_folder)
                count = count + 1
        if success and not invalid:
            # Insert query
            insert_query = """
            INSERT INTO crop_health (date, username, field_id, ndwi, ndvi, savi, evi, area, crop_type, cloud)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            for row in results:
                date = current_date
                polygon = json.loads(row['polygon_geojson'])
                username = row['username']
                field_id = row['field_id']
                zone_number = sentinel2_query[6:8]
                ndwi, ndvi, savi, evi, area = calculate_average_indicators_for_polygon(polygon['coordinates'][0], file_paths[0], file_paths[1], zone_number)
                # Ensure all NumPy types are converted to native Python types
                ndwi, ndvi, savi, evi= float(ndwi), float(ndvi), float(savi), float(evi)
                cloud = "no"
                crop_type = row['crop_type']
                # Insert data into the table
                try:
                    cursor.execute(insert_query, (date, username, field_id, ndwi, ndvi, savi, evi, area, crop_type, cloud))
                    conn.commit()
                    print(f"Inserted data for username: {username}, field_id: {field_id}")
                except Exception as e:
                    conn.rollback()
                    print(f"Error inserting data for username: {username}, field_id: {field_id}. Error: {e}")

        if invalid or not success:
            # Move to the next day
            current_date += timedelta(days=1)
        else:
            current_date += timedelta(days=5)
else:
    print("No data found for the specified username.")
