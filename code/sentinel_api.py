import pyproj
import rasterio
import numpy as np
from shapely.geometry import Polygon, Point
from rasterio.windows import from_bounds
from rasterio.windows import transform as window_transform
import psycopg2
from psycopg2.extras import DictCursor

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

import boto3
import os
import psycopg2
from datetime import datetime, timedelta
import json

# S3 Configuration
BUCKET_NAME = "sentinel-s2-l2a"
LOCAL_DIRECTORY = r"C:\Users\Javier\Desktop\CROP\Sentinel2_trial"

# Create the directory if it doesn't exist
os.makedirs(LOCAL_DIRECTORY, exist_ok=True)

# PostgreSQL Connection
conn = psycopg2.connect(
    dbname='crop-health-db',
    user='master',
    password='JFKae$2341',
    host='crop-health-db.cv0iskeoocuw.eu-north-1.rds.amazonaws.com',
    port='5432'
)
cursor = conn.cursor(cursor_factory=DictCursor)

# Step 1: Retrieve the rows for a specific client_id
client_id = "0000"  # Replace this with the actual client_id you are filtering by

cursor.execute("""
    SELECT 
        *, 
        ST_AsGeoJSON(polygon) AS polygon_geojson 
    FROM fields 
    WHERE client_id = %s;
""", (client_id,))

results = cursor.fetchall()  # Fetch all rows

if results:
    # Find the oldest created_at date
    oldest_date = None
    sentinel2_query = None

    for row in results:
        created_at = row[4]  # Assuming created_at is the 5th column
        if oldest_date is None or created_at < oldest_date:
            oldest_date = created_at
            sentinel2_query = row[5]  # Assuming sentinel2_query is the 6th column

    print(f"Oldest created_at: {oldest_date}")
    
    # Step 2: Calculate the starting_date (one year before oldest_date)
    starting_date = oldest_date - timedelta(days=365)
    print(f"Starting date (one year before): {starting_date}")

    # Format the starting date for file path (e.g., 'YYYY/M/D')
    starting_date_str = f"{starting_date.year}/{starting_date.month}/{starting_date.day}"

    print(f"Sentinel-2 query: {sentinel2_query}")

    # Step 4: Initialize S3 client and download files
    s3_client = boto3.client("s3")

    # Retry logic: keep trying until the files are successfully downloaded
    keep_trying = True
    while keep_trying:
        # Construct file paths using the starting_date
        FILES_TO_DOWNLOAD = [
            f"{sentinel2_query}{starting_date_str}/0/qi/CLD_20m.jp2",
            f"{sentinel2_query}{starting_date_str}/0/R10m/B08.jp2",
            f"{sentinel2_query}{starting_date_str}/0/R10m/TCI.jp2"
        ]
        file_paths = []
        files_downloaded = 0  # Keep track of successfully downloaded files
        for file_key in FILES_TO_DOWNLOAD:
            local_file_path = os.path.join(LOCAL_DIRECTORY, os.path.basename(file_key))
            file_paths.append(local_file_path)
            try:
                print(f"Downloading {file_key} to {local_file_path}...")
                s3_client.download_file(BUCKET_NAME, file_key, local_file_path)
                print(f"Downloaded {file_key} successfully.")
                files_downloaded += 1
            except Exception as e:
                print(f"Error downloading {file_key}: {e}")

        # If all files are successfully downloaded, exit the loop
        if files_downloaded == len(FILES_TO_DOWNLOAD):
            print("All files downloaded successfully.")
            keep_trying = False
        else:
            # If some files failed, increment the starting date and try again
            print("Retrying with the next day...")
            starting_date += timedelta(days=1)
            starting_date_str = f"{starting_date.year}/{starting_date.month}/{starting_date.day}"
    
else:
    print(f"No results found for client_id {client_id}")

# Insert query
insert_query = """
INSERT INTO crop_health (date, client_id, field_id, issues, ndwi, ndvi, savi, evi, area, crop_type)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
"""
for row in results:
    date = starting_date
    polygon = json.loads(row['polygon_geojson'])
    client_id = row['client_id']
    field_id = row['field_id']
    zone_number = sentinel2_query[6:8]
    ndwi, ndvi, savi, evi, area = calculate_average_indicators_for_polygon(polygon['coordinates'][0], file_paths[2], file_paths[1], zone_number)
    # Ensure all NumPy types are converted to native Python types
    ndwi, ndvi, savi, evi= float(ndwi), float(ndvi), float(savi), float(evi)
    issues = ""
    crop_type = row['crop_type']
    # Insert data into the table
    try:
        cursor.execute(insert_query, (date, client_id, field_id, issues, ndwi, ndvi, savi, evi, area, crop_type))
        conn.commit()
        print(f"Inserted data for client_id: {client_id}, field_id: {field_id}")
    except Exception as e:
        conn.rollback()
        print(f"Error inserting data for client_id: {client_id}, field_id: {field_id}. Error: {e}")


    
    # Close the database connection
cursor.close()
conn.close()
