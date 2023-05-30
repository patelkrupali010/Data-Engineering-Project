import json
from jsonschema import validate, ValidationError
import pandas as pd
import numpy as np
import os
from datetime import datetime
import mysql.connector
from sqlalchemy import create_engine
import csv
from typing import List, Tuple

# Define the database connection parameters
DB_CONFIG = {
    'host': os.environ.get('DB_HOST'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'database': os.environ.get('DB_DATABASE'),
    'port': int(os.environ.get('DB_PORT'))
}

JSON_ERROR_FOLDER = "roof_json_errors"

# Create the json error folder if it does not exist
if not os.path.exists(JSON_ERROR_FOLDER):
    os.mkdir(JSON_ERROR_FOLDER)

JSON_SCHEMA_ERROR_FILENAME = "json_schema_errors.csv"
JSON_LINT_ERROR_FILENAME = "json_lint_errors.csv"
DB_TABLES_FILENAME = "roof_data_dictionary.csv"


def create_database(cursor, db_name):
    """Create a database if it doesn't exist."""
    cursor.execute("CREATE DATABASE IF NOT EXISTS {}".format(db_name))

def create_connection():
    """Create a new connection object with autocommit enabled."""
    cnx = mysql.connector.connect(host=DB_CONFIG['host'], user=DB_CONFIG['user'], password=DB_CONFIG['password'], autocommit=True)
    return cnx

def connect_to_database():
    """Connect to the database."""
    cnx = create_connection()
    create_database(cnx.cursor(), DB_CONFIG['database'])
    cnx.close()
    cnx = mysql.connector.connect(**DB_CONFIG)
    return cnx

def create_error_file(file_path):
    """
    Create a CSV file to store error messages and return a writer object to the file.
    Args:
        file_path (str): The path to the CSV file to create.
    Returns:
        tuple: A tuple containing the file object and a writer object.
    """

    json_error_file = open(file_path, 'w')
    error_writer = csv.writer(json_error_file)
    error_writer.writerow(['Filename', 'Error Message'])
    return json_error_file, error_writer

def process_json_files(folder_path):
    """
    Returns a list of JSON files in a given folder.
    Args:
        folder_path (str): The path to the folder containing the JSON files.
    Returns:
        list: A list of JSON file names.
    """
    json_files = [file for file in os.listdir(folder_path) if file.endswith('.json')]
    return json_files

def validate_json_schema(file_name: str, json_data: dict) -> bool:
    """
    Validates the given JSON object against a schema defined in a schema.json file.
    If the JSON object is invalid, it appends the error message to a csv file named json_schema_errors.csv.

    Args:
     - param file_name: The name of the JSON file being validated.
     - param json_data: The JSON object to be validated.

    Returns :
     - bool: True if the JSON object is valid, False otherwise.
    """
    # Define the path to the schema file
    schema_file_path = os.path.join(os.getcwd(), 'schema.json')
    
    # Load the schema file
    with open(schema_file_path, 'r') as f:
        json_schema_data = json.load(f)

    # Validate the JSON object against the schema
    try:
        validate(instance=json_data, schema=json_schema_data)
        return True
    except ValidationError as e:
        err_msg = f"The JSON object is invalid: {e.message}"

        # Append error messages to the CSV file
        error_file_path = os.path.join(JSON_ERROR_FOLDER, JSON_SCHEMA_ERROR_FILENAME)
        with open(error_file_path, 'a', newline='') as json_error_file:
            if json_error_file.closed:
                print("File is already closed")
            csv_writer = csv.writer(json_error_file)
            csv_writer.writerow([file_name, err_msg])

        return False

def transform_data(json_data: dict) -> Tuple[List[dict], List[dict], List[dict], List[dict], List[dict], List[dict]]:
    """
    Extracts data from the given JSON object and returns lists of dictionaries for each data category.

    Args:
    - json_data (dict): The JSON object to be processed.

    Returns:
    - tuple: A tuple containing lists of dictionaries for site, buildings, mounting planes, edges, penetrations, and obstructions.
    """
    site_id = json_data.get('id')
    site_model = json_data.get('siteModel', {})
    date_created = datetime.fromisoformat(json_data.get('dateCreated', '').replace('Z', '+00:00')) if json_data.get('dateCreated') else None

        
    # Initialize list for site data
    site_lst = []
    # Extract site data
    site_data = {
        'site_id': site_id,
        'installationId': json_data.get('installationId'),
        'dateCreated': date_created,
        'version': json_data.get('version'),
        'length_unit': site_model.get('units', {}).get('length', []),
        'angle_unit': site_model.get('units', {}).get('angle'),
        'area_unit': site_model.get('units', {}).get('area'),
        'northVector_x': site_model.get('northVector', {}).get('x'),
        'northVector_y': site_model.get('northVector', {}).get('y'),
        'northVector_z': site_model.get('northVector', {}).get('z'),
        'headingVector_x': site_model.get('headingVector', {}).get('x'),
        'headingVector_y': site_model.get('headingVector', {}).get('y'),
        'headingVector_z': site_model.get('headingVector', {}).get('z'),
        'externalSiteModelSourceId': json_data.get('externalSiteModelSourceId'),
        'etlUpdatedDate': datetime.now(),
    }
    
    site_lst.append(site_data)

    # Extract outer obstructions data - sitemodel level
    obstructions_lst = []
    if obstructions := site_model.get('obstructions'):
        for obstruction in obstructions:
            outer_obstruction_data = {
                'site_id': site_id,
                'building_id': None,
                'mounting_plane_id': None,
                'obstruction_id': obstruction['id'],
                'shapeType': obstruction['shapeType'],
                'featureName': obstruction['featureName'],
                'radius': obstruction.get('radius', np.nan),
                'level': 'site_level'
            }
            obstructions_lst.append(outer_obstruction_data)
 
    # Extracting building, mounting plane, edge, penetration and obstruction data
    bld_lst = []
    mountain_planes_lst = []
    edges_lst = []
    penetration_lst = []
    for building_id, building in enumerate(site_model.get('buildings', []), start=1):
        # Extract building data
        building_data = {
            'site_id': site_id,
            'building_id': building_id,
            'is_primary_building': building.get('isPrimaryBuilding'),
            'total_roof_area': building.get('totalRoofArea'),
            'etlUpdatedDate': datetime.now(),
        }
        bld_lst.append(building_data)

        # Extract data for each mounting plane of the building
        for plane in building.get('mountingPlanes', []):
            # Extracting mounting plane data
            plane_data = {
                'site_id': site_id,
                'building_id': building_id,
                'mounting_plane_id': plane.get('id'),
                'area': plane.get('area'),
                'pitch_angle': float(plane.get('pitchAngle', -1)),
                'azimuth_angle': float(plane.get('azimuthAngle', -1)),
                'centroid_x': plane.get('centroid', {}).get('x'),
                'centroid_y': plane.get('centroid', {}).get('y'),
                'centroid_z': plane.get('centroid', {}).get('z'),
                'azimuthVector_x': plane['azimuthVector'].get('x', np.nan),
                'azimuthVector_y': plane['azimuthVector'].get('y', np.nan),
                'azimuthVector_z': plane['azimuthVector'].get('z', np.nan),
                'coordinateSystem_x_Axis_x': plane['coordinateSystem']['xAxis'].get('x', np.nan),
                'coordinateSystem_x_Axis_y': plane['coordinateSystem']['xAxis'].get('y', np.nan),
                'coordinateSystem_x_Axis_z': plane['coordinateSystem']['xAxis'].get('z', np.nan),
                'coordinateSystem_y_Axis_x': plane['coordinateSystem']['yAxis'].get('x', np.nan),
                'coordinateSystem_y_Axis_y': plane['coordinateSystem']['yAxis'].get('y', np.nan),
                'coordinateSystem_y_Axis_z': plane['coordinateSystem']['yAxis'].get('z', np.nan),
                'coordinateSystem_z_Axis_x': plane['coordinateSystem']['zAxis'].get('x', np.nan),
                'coordinateSystem_z_Axis_y': plane['coordinateSystem']['zAxis'].get('y', np.nan),
                'coordinateSystem_z_Axis_z': plane['coordinateSystem']['zAxis'].get('z', np.nan),
                'polygon_exteriorRing_windingDirection': plane['polygon']['exteriorRing'].get('windingDirection', np.nan),
                "roof_material_type": plane.get('roofMaterialType', None),
                'etlUpdatedDate': datetime.now()
            }
            mountain_planes_lst.append(plane_data)
        
            # Extract all edges for a polygon in a mounting plane
            for edge in plane['polygon']['exteriorRing'].get('edges', np.nan):
                edge_data = {
                    'site_id': site_id,
                    'building_id': building_id,
                    'edge_id': edge['id'],
                    'startPoint_x': edge['startPoint'].get('x', np.nan),
                    'startPoint_y': edge['startPoint'].get('y', np.nan),
                    'startPoint_z': edge['startPoint'].get('z', np.nan),
                    'endPoint_x': edge['endPoint'].get('x', np.nan),
                    'endPoint_y': edge['endPoint'].get('y', np.nan),
                    'endPoint_z': edge['endPoint'].get('z', np.nan),
                    'bearingVector': edge.get('bearingVector', np.nan),
                    'angleBetweenBearingVectorAndUpVector': edge.get('angleBetweenBearingVectorAndUpVector', np.nan),
                    'angleBetweenBearingVectorAndRightVector': edge.get('angleBetweenBearingVectorAndRightVector', np.nan),
                    'edgeCondition': edge.get('edgeCondition', np.nan),
                    'sidingMaterial': edge.get('sidingMaterial', np.nan),
                }
                edges_lst.append(edge_data)

            # Extract penetrations data    
            if plane.get('penetrations', []) is not None:
                for penetration in plane.get('penetrations', []):
                    penetration_data = {
                        'site_id': site_id,
                        'building_id': building_id,
                        "mounting_plane_id": plane.get('id', None),
                        'penetration_id': penetration['id'],
                        'obstructionId': penetration['obstructionId']
                    }
                    penetration_lst.append(penetration_data)
                
                if plane.get('obstructions', []) is not None:
                    for obstruction in plane.get('obstructions', []):
                        obstruction_data = {
                            'site_id': site_id,
                            'building_id': building_id,
                            "mounting_plane_id": plane.get('id', None),
                            'obstruction_id': obstruction['id'],
                            'shapeType': obstruction['shapeType'],
                            'featureName': obstruction['shapeType'],
                            'center_x': obstruction['center'].get('x'),
                            'center_y': obstruction['center'].get('y'),
                            'center_z': obstruction['center'].get('z'),
                            'radius': obstruction['radius'],
                            'level': 'plane_level'
                        }
                        obstructions_lst.append(obstruction_data)


    return site_lst, bld_lst, mountain_planes_lst, edges_lst, penetration_lst, obstructions_lst

def write_dataframe_to_database(df, table_name, engine):
    """
    Writes a Pandas DataFrame to a MySQL database table.

    Args:
    - df (pandas.DataFrame): The DataFrame to be written to the database.
    - table_name (str): The name of the table in the database to which the DataFrame will be written.
    - engine (sqlalchemy.engine.base.Engine): An SQLAlchemy database engine object representing the database connection.

    Returns:
    - None
    """
    # Use the to_sql method of the Pandas DataFrame object to write the data to the specified database table
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)

def write_data_dictionary_to_csv(cnx, DB_CONFIG, output_file_path):
    """
    Writes the data dictionary for the database to a CSV file.

    Args:
    - cnx (mysql.connector.connect): A connection object to the MySQL database.
    - DB_CONFIG (dict): A dictionary containing the database connection parameters.
    - output_file_path (str): The path to the output CSV file.

    Returns:
    None
    """

    # Open the output file and create a csv writer
    with open(output_file_path, 'w', newline='') as output_file:
        csv_writer = csv.writer(output_file)

        # Execute the query to retrieve the table and column names for the database
        query = """
        SELECT TABLE_NAME, COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{}'
        ORDER BY TABLE_NAME, ORDINAL_POSITION
        """.format(DB_CONFIG['database'])

        cursor = cnx.cursor()
        cursor.execute(query)

        # Write the table and column names to the output CSV file
        csv_writer.writerow(['Table Name', 'Column Name'])
        for row in cursor.fetchall():
            csv_writer.writerow(row)

        cursor.close()


def main():
    """
    This function reads in JSON files from a specified folder path, validates their schema,
    transforms the data, and then writes the resulting data to a MySQL database and a CSV file.
    Additionally, it writes any errors encountered during the process to a separate CSV file.
    """

    # Set the path to the folder containing the JSON files
    folder_path = 'roof_input_data'

    # Get a list of all JSON files in the folder
    json_files = process_json_files(folder_path)

    # Connect to the database
    cnx = connect_to_database()

    # Create a file to write JSON errors to
    json_error_file, error_writer = create_error_file(os.path.join(JSON_ERROR_FOLDER, JSON_LINT_ERROR_FILENAME))

    # Create an engine to connect to the database
    engine = create_engine('mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}'.format(**DB_CONFIG))

    # Create a dictionary to store data for each table
    data = {'sites': [], 'buildings': [], 'mounting_planes': [], 'edges': [], 'penetrations': [], 'obstructions': []}

    # Loop through each JSON file in the folder
    for file in json_files:
        print(file)
        file_path = os.path.join(folder_path, file)

        try:
            # Open the file and load the JSON data
            with open(file_path, 'r') as f:
                fileName = os.path.basename(f.name)
                json_data = json.load(f)

                # Validate the JSON data against the schema
                if validate_json_schema(fileName, json_data):
                    # Transform the JSON data into lists of data for each table
                    site_lst, bld_lst, mountain_planes_lst, edges_lst, penetration_lst, obstructions_lst = transform_data(json_data)

                    # Check that data was transformed successfully
                    if transform_data(json_data):
                        # Add the data to the dictionary for each table
                        data['sites'].extend(site_lst)
                        data['buildings'].extend(bld_lst)
                        data['mounting_planes'].extend(mountain_planes_lst)
                        data['edges'].extend(edges_lst)
                        data['penetrations'].extend(penetration_lst)
                        data['obstructions'].extend(obstructions_lst)
        except ValueError as e:
            # If there was an error loading the JSON file, write it to the error file
            error_writer.writerow([fileName, f'Invalid JSON format in file: {fileName}: {e}'])

    # Convert the dictionary of data into a dictionary of Pandas dataframes
    dataframes = {}
    for key in data:
        dataframes[key] = pd.DataFrame(data[key])

    # Write each dataframe to the database as a table
    for key, df in dataframes.items():
        write_dataframe_to_database(df, key, engine)


    write_data_dictionary_to_csv(cnx, DB_CONFIG, DB_TABLES_FILENAME)

    # Commit the changes
    cnx.commit()

    # Close the error file, database cursor, and connection
    json_error_file.close()
    cnx.close()

if __name__ == "__main__":
    main()
