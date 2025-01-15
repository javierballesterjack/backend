import pyproj
import rasterio
import numpy as np
from shapely.geometry import Polygon, Point
from rasterio.windows import from_bounds
from rasterio.windows import transform as window_transform
from math import ceil


def lonlat_to_utm(lon, lat, zone_number):
    """Convert longitude and latitude to UTM coordinates."""
    wgs84_proj = pyproj.CRS("EPSG:4326")  # WGS84 (longitude, latitude)
    utm_proj = pyproj.CRS(f"EPSG:326{zone_number}")  # UTM Zone 30T

    transformer = pyproj.Transformer.from_crs(wgs84_proj, utm_proj, always_xy=True)
    return transformer.transform(lon, lat)

def convert_polygon_to_utm(polygon, zone_number):
    """Convert a list of polygon points from lon/lat to UTM (meters)."""
    return [lonlat_to_utm(lon, lat, zone_number) for lon, lat in polygon]

def get_points_in_polygon(polygon, x_vals, y_vals, ndwi_values, ndvi_values):
    """Get the values of grid points inside the polygon."""
    polygon_shape = Polygon(polygon)
    ndwi_list = []
    ndvi_list = []
    # Generate a 2D grid of coordinates
    grid_x, grid_y = np.meshgrid(x_vals, y_vals)
    grid_x_flat = grid_x.flatten()
    grid_y_flat = grid_y.flatten()

    # Ensure values are flattened to match the grid
    ndwi_values_flat = ndwi_values.flatten()
    ndvi_values_flat = ndvi_values.flatten()
    for x, y, ndwi_val, ndvi_val in zip(grid_x_flat, grid_y_flat, ndwi_values_flat, ndvi_values_flat):
        point = Point(x, y)
        if polygon_shape.contains(point):
            ndwi_list.append(ndwi_val)
            ndvi_list.append(ndvi_val)
        
    return ndwi_list, ndvi_list

def calculate_average_indicators_for_polygon(polygon, tci_image_path, nir_image_path,cloud_image_path, zone_number):
    """Calculate average NDWI for the given polygon."""
    
    # Open the TCI image and NIR image
    with rasterio.open(tci_image_path) as src_tci, rasterio.open(nir_image_path) as src_nir, rasterio.open(cloud_image_path) as src_cloud:

        # Convert polygon to UTM and calculate the bounding box
        polygon_utm = convert_polygon_to_utm(polygon, zone_number)
        all_x = [coord[0] for coord in polygon_utm]
        all_y = [coord[1] for coord in polygon_utm]
        min_x, max_x = min(all_x), max(all_x)
        min_y, max_y = min(all_y), max(all_y)
        cloud_image = src_cloud.read(1)
        cloud_image_high_res = np.repeat(np.repeat(cloud_image, 2, axis=0), 2, axis=1)
        
        # Read a subset of the image using the bounding box
        polygon_window = from_bounds(min_x, min_y, max_x, max_y, src_tci.transform)
        subset_rgb_image = src_tci.read([1, 2, 3], window=polygon_window)
        subset_nir_image = src_nir.read(1, window=polygon_window)  # Read NIR band for subset
        

        
        print(polygon_window.row_off)
        print(polygon_window.height)
        print(polygon_window.col_off)
        print(polygon_window.width)
        print(f'original shape: {subset_nir_image.shape}, cloud shape {subset_cloud_image.shape}')
        
        subset_ndwi_image = (subset_rgb_image[1, :, :] - subset_nir_image) / (subset_rgb_image[1, :, :] + subset_nir_image)
        subset_ndvi_image = (subset_nir_image - subset_rgb_image[0, :, :]) / (subset_nir_image + subset_rgb_image[0, :, :])
        # Get the transform for the subset
        subset_transform = window_transform(polygon_window, src_tci.transform)

        # Generate 1D arrays for the x and y coordinates
        cols = subset_ndwi_image.shape[1]  # Number of columns
        rows = subset_ndwi_image.shape[0]  # Number of rows

        x_vals = np.arange(cols) * subset_transform[0] + subset_transform[2] + 5  # X coordinates
        y_vals = np.arange(rows) * subset_transform[4] + subset_transform[5] - 5  # Y coordinates

        # Get points inside the polygon
        ndwi_list, ndvi_list = get_points_in_polygon(
            polygon_utm, x_vals, y_vals, subset_ndwi_image, subset_ndvi_image
        )

        # Calculate the average NDWI for the polygon
        if ndwi_list:
            average_ndwi = np.mean(ndwi_list)
            average_ndvi = np.mean(ndvi_list)
            return average_ndwi, average_ndvi, len(ndwi_list)
        else:
            return None

# Example usage
polygons = [
    [
        [-5.659101875844044, 40.11420457759803],
        [-5.6586607530656465, 40.11445950925406],
        [-5.658814639440891, 40.11480235562172],
        [-5.659329025592882, 40.114648418294564],
        [-5.659101875844044, 40.11420457759803]
    ],
    [
        [-5.65895536574493, 40.114246368153005],
        [-5.658932197777517, 40.11415600648127],
        [-5.658726002866388, 40.1140479266777],
        [-5.658232525159747, 40.11449264739281],
        [-5.6583251970292565, 40.11455643179909],
        [-5.65851517436252, 40.11440760142537],
        [-5.658702834899032, 40.114396970671976],
        [-5.65895536574493, 40.114246368153005]
    ],
    [
        [-5.658595234262606, 40.11345930492428],
        [-5.658060823345011, 40.113190705222706],
        [-5.658071312624372, 40.11351920030063],
        [-5.657992994441571, 40.113597683816494],
        [-5.657766141773379, 40.113705082165836],
        [-5.657444767159518, 40.11414500055585],
        [-5.657520384716179, 40.11427718205161],
        [-5.658190140212895, 40.114518825684456],
        [-5.658705959970149, 40.114027276195486],
        [-5.658678953700246, 40.11363072948032],
        [-5.658595234262606, 40.11345930492428]
    ],
    [
    [-5.658601524001199, 40.113452322615444],  
    [-5.658687558598899, 40.1136276788269],    
    [-5.6587224712473585, 40.11402016890898],  
    [-5.658952894728657, 40.11415366842007],  
    [-5.659485312620603, 40.11396810402778],   
    [-5.659274193316946, 40.113383861328714],  
    [-5.658849618767761, 40.11330757581305],   
    [-5.658601524001199, 40.113452322615444]   
]
]
tci_image_path = "C:\\Users\\Javier\\Desktop\\CROP\\Sentinel2_trial\\TCI.jp2"
nir_image_path = "C:\\Users\\Javier\\Desktop\\CROP\\Sentinel2_trial\\B08.jp2"
cloud_image_path = "C:\\Users\\Javier\\Desktop\\CROP\\Sentinel2_trial\\CLD_20m.jp2"
zone_number = 30

total_points = 0
weighted_sum_ndwi = 0
weighted_sum_ndvi = 0
for polygon in polygons:
    average_ndwi, average_ndvi, points_polygon = calculate_average_indicators_for_polygon(polygon, tci_image_path, nir_image_path,cloud_image_path, zone_number)
    weighted_sum_ndwi = weighted_sum_ndwi + points_polygon * average_ndwi
    weighted_sum_ndvi = weighted_sum_ndvi + points_polygon * average_ndvi
    total_points = total_points + points_polygon
    if average_ndwi is not None:
        print(f"Average NDWI for polygon: {average_ndwi:.4f}")
        print(f"Average NDVI for polygon: {average_ndvi:.4f}")
    else:
        print("No grid points inside the polygon.")
total_ndwi = weighted_sum_ndwi / total_points
total_ndvi = weighted_sum_ndvi / total_points
print(f"Average NDWI for all polygons: {total_ndwi:.4f}")
print(f"Average NDWI for all polygons: {total_ndvi:.4f}")