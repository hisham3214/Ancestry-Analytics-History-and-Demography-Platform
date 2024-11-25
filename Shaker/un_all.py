import json
import mysql.connector
import sys
import os

# Add the parent directory to the system path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from main import config  # Import the database configuration from main.py

class CountryAPI:
    @staticmethod
    def ensure_data_source():
        """
        Ensure there is at least one data source in the Data_Sources table.
        Returns the `source_id` of an existing or newly created source.
        """
        try:
            cnx = mysql.connector.connect(**config)
            cursor = cnx.cursor()

            # Check if there is already a data source
            cursor.execute("SELECT source_id FROM Data_Sources LIMIT 1")
            row = cursor.fetchone()
            if row:
                return row[0]

            # Insert a default data source if none exists
            insert_source_query = """
                INSERT INTO Data_Sources (name, website)
                VALUES (%s, %s)
            """
            cursor.execute(insert_source_query, ("Default Source", "https://restcountries.com"))
            cnx.commit()
            return cursor.lastrowid
        except mysql.connector.Error as err:
            print(f"Database error: {err}")
        finally:
            cursor.close()
            cnx.close()

    @staticmethod
    def add_country_and_population_to_database(country_data):
        try:
            # Extract necessary fields
            country_name = country_data['name']['common']
            country_code = country_data.get('cca3', 'N/A')  # 'cca3' is the 3-letter country code
            population = country_data.get('population', 0)  # Population defaults to 0 if missing

            # Ensure a valid data source exists
            source_id = CountryAPI.ensure_data_source()

            # Establish a database connection using the config from main.py
            cnx = mysql.connector.connect(**config)
            cursor = cnx.cursor()

            # Check if the country already exists in the database
            check_country_query = """
                SELECT country_id FROM Countries WHERE country_name = %s OR country_code = %s
            """
            cursor.execute(check_country_query, (country_name, country_code))
            result = cursor.fetchone()

            if result:
                country_id = result[0]
                print(f"Country '{country_name}' already exists in the database.")
            else:
                # Insert country data into the Countries table
                insert_country_query = """
                    INSERT INTO Countries (country_name, country_code)
                    VALUES (%s, %s)
                """
                cursor.execute(insert_country_query, (country_name, country_code))
                country_id = cursor.lastrowid
                print(f"Inserted country '{country_name}' into the database.")

            # Check if population data for the current year already exists
            check_population_query = """
                SELECT * FROM Population WHERE country_id = %s AND year = %s
            """
            current_year = 2024  # Update as needed
            cursor.execute(check_population_query, (country_id, current_year))
            population_result = cursor.fetchone()

            if population_result:
                print(f"Population data for '{country_name}' in {current_year} already exists.")
            else:
                # Insert population data into the Population table
                insert_population_query = """
                    INSERT INTO Population (country_id, source_id, year, population)
                    VALUES (%s, %s, %s, %s)
                """
                cursor.execute(insert_population_query, (country_id, source_id, current_year, population))
                print(f"Inserted population data for '{country_name}' in {current_year}.")

            # Commit the changes
            cnx.commit()
        except mysql.connector.Error as err:
            print(f"Database error: {err}")
        except KeyError as e:
            print(f"Key error: {e} not found in the country data.")
        finally:
            # Close the cursor and connection
            cursor.close()
            cnx.close()

if __name__ == "__main__":
    # Read the JSON file containing all countries data
    try:
        with open("countries_data.json", "r", encoding="utf-8") as file:
            countries_data = json.load(file)
    except FileNotFoundError:
        print("The JSON file 'countries_data.json' was not found.")
        exit(1)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        exit(1)

    # Process each country in the data
    for country_data in countries_data:
        country_name = country_data['name']['common']
        print(f"Processing {country_name}...")
        CountryAPI.add_country_and_population_to_database(country_data)
        print(f"Finished processing {country_name}.\n")
