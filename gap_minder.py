import mysql.connector
import pandas as pd
from mysql.connector import errorcode

# MySQL connection details
config = {
    'user': 'root',  # Your MySQL username
    'password': 'LZ#amhe!32',  # Your MySQL password
    'host': '127.0.0.1',  # The host where MySQL server is running
    'database': 'fyp1',  # The database name
    'raise_on_warnings': True
}

def get_or_create_source(cursor):
    """Ensure the data source exists and return its ID."""
    source_name = "GM-Population"
    website = "https://www.gapminder.org/data/documentation/gd003/"
    
    # Check if the source already exists
    cursor.execute("SELECT source_id FROM Data_Sources WHERE name = %s", (source_name,))
    result = cursor.fetchone()
    
    if result:
        cursor.fetchall()  # Ensure any remaining results are cleared
        return result[0]
    
    # Insert the source if not exists
    cursor.execute("INSERT INTO Data_Sources (name, website) VALUES (%s, %s)", (source_name, website))
    return cursor.lastrowid

def get_or_create_country(cursor, country_name, country_code):
    """Ensure the country exists and return its ID."""
    cursor.execute("SELECT country_id FROM Countries WHERE country_code = %s", (country_code,))
    result = cursor.fetchone()
    
    if result:
        cursor.fetchall()  # Ensure any remaining results are cleared
        return result[0]
    
    # Insert the country if not exists
    cursor.execute("INSERT INTO Countries (country_name, country_code) VALUES (%s, %s)", (country_name, country_code))
    return cursor.lastrowid

import numpy as np

def insert_population_data(file_path):
    """Insert population data from CSV into the database."""
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    
    try:
        # Load CSV file
        df = pd.read_csv(file_path)

        # Drop rows where any required column is missing
        df = df.dropna(subset=['geo', 'name', 'time', 'Population'])

        # Convert year and population columns to the correct data type
        df['time'] = df['time'].astype(int)
        df['Population'] = df['Population'].replace({np.nan: None})  # Replace NaN with None (NULL in MySQL)

        # **Filter: Ignore data beyond year 1950**
        df = df[df['time'] <= 1950]
        
        # Ensure source exists
        source_id = get_or_create_source(cursor)
        
        for _, row in df.iterrows():
            country_id = get_or_create_country(cursor, row['name'], row['geo'])
            
            if row['Population'] is not None:  # Avoid inserting NULL values if Population is missing
                cursor.execute("""
                    INSERT INTO Population (country_id, source_id, year, population)
                    VALUES (%s, %s, %s, %s)
                """, (country_id, source_id, row['time'], row['Population']))
        
        cnx.commit()
        print("Data inserted successfully!")
    
    except mysql.connector.Error as err:
        print("Error:", err)
    
    finally:
        cursor.close()
        cnx.close()

# Example usage
file_path = "GM-Population - Dataset - v8 - data-for-countries-etc-by-year.csv"  # Replace with your actual CSV file path
insert_population_data(file_path)
