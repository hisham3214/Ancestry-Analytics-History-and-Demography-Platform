import requests
import mysql.connector
from main import config  # Import the database configuration from main.py

class CountryAPI:
    BASE_URL = "https://restcountries.com/v3.1/name/"

    @staticmethod
    def get_country_data(country_name):
        try:
            response = requests.get(CountryAPI.BASE_URL + country_name)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
        except Exception as err:
            print(f"Other error occurred: {err}")
        return None

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
    def add_country_and_population_to_database(country_name):
        # Fetch country data from the API
        data = CountryAPI.get_country_data(country_name)
        if not data:
            print("Failed to fetch country data.")
            return

        try:
            # Extract necessary fields
            country_data = {
                "country_name": data[0]['name']['common'],
                "country_code": data[0].get('cca3', 'N/A'),  # 'cca3' is the 3-letter country code
                "population": data[0].get('population', 0),  # Population defaults to 0 if missing
            }

            # Ensure a valid data source exists
            source_id = CountryAPI.ensure_data_source()

            # Establish a database connection using the config from main.py
            cnx = mysql.connector.connect(**config)
            cursor = cnx.cursor()

            # Insert country data into the Countries table
            insert_country_query = """
                INSERT INTO Countries (country_name, country_code)
                VALUES (%s, %s)
            """
            cursor.execute(insert_country_query, (country_data["country_name"], country_data["country_code"]))

            # Get the ID of the newly inserted country
            country_id = cursor.lastrowid

            # Insert population data into the Population table
            insert_population_query = """
                INSERT INTO Population (country_id, source_id, year, population)
                VALUES (%s, %s, %s, %s)
            """
            cursor.execute(insert_population_query, (country_id, source_id, 2024, country_data["population"]))

            # Commit the changes
            cnx.commit()
            print(f"Country '{country_data['country_name']}' and its population added to the database.")
        except mysql.connector.Error as err:
            print(f"Database error: {err}")
        finally:
            # Close the cursor and connection
            cursor.close()
            cnx.close()

if __name__ == "__main__":
    # List of countries to automatically process
    countries = ["France", "Germany", "Spain", "Canada", "Japan"]
    
    for country in countries:
        print(f"Processing {country}...")
        CountryAPI.add_country_and_population_to_database(country)
        print(f"Finished processing {country}.")
