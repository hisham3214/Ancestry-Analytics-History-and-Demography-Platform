import sys
import logging
import requests
from datetime import datetime
from typing import Dict, Any, List
from contextlib import contextmanager
import mysql.connector
from time import sleep
from requests.exceptions import RequestException

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database configuration
config = {
    'user': 'root',
    'password': 'LZ#amhe!32',
    'host': '127.0.0.1',
    'database': 'fyp',
    'raise_on_warnings': True
}

@contextmanager
def get_db_connection():
    """Context manager for database connections."""
    cnx = mysql.connector.connect(**config)
    try:
        yield cnx
    finally:
        cnx.close()

class PopulationAPI:
    API_URL = "https://restcountries.com/v3.1/all"
    SOURCE_NAME = "REST Countries API"
    SOURCE_WEBSITE = "https://restcountries.com"
    
    @staticmethod
    def fetch_countries() -> List[Dict[str, Any]]:
        """Fetch country data from REST Countries API."""
        try:
            response = requests.get(PopulationAPI.API_URL)
            response.raise_for_status()
            return response.json()
        except RequestException as e:
            logger.error(f"Failed to fetch data from API: {e}")
            raise

    @staticmethod
    def ensure_data_source() -> int:
        """Ensure data source exists in database and return its ID."""
        try:
            with get_db_connection() as cnx:
                with cnx.cursor() as cursor:
                    # Check if source already exists
                    cursor.execute(
                        "SELECT source_id FROM Data_Sources WHERE website = %s",
                        (PopulationAPI.SOURCE_WEBSITE,)
                    )
                    result = cursor.fetchone()
                    if result:
                        return result[0]

                    # Create new source
                    cursor.execute(
                        "INSERT INTO Data_Sources (name, website) VALUES (%s, %s)",
                        (PopulationAPI.SOURCE_NAME, PopulationAPI.SOURCE_WEBSITE)
                    )
                    cnx.commit()
                    return cursor.lastrowid

        except mysql.connector.Error as err:
            logger.error(f"Database error in ensure_data_source: {err}")
            raise

    @staticmethod
    def process_country(country_data: Dict[str, Any], source_id: int) -> None:
        """Process and store country and population data."""
        try:
            country_name = country_data['name']['common']
            country_code = country_data.get('cca3', 'N/A')
            population = country_data.get('population', 0)
            current_year = datetime.now().year

            with get_db_connection() as cnx:
                with cnx.cursor() as cursor:
                    # Insert or get country
                    cursor.execute(
                        "SELECT country_id FROM Countries WHERE country_code = %s",
                        (country_code,)
                    )
                    result = cursor.fetchone()
                    
                    if result:
                        country_id = result[0]
                    else:
                        cursor.execute(
                            "INSERT INTO Countries (country_name, country_code) VALUES (%s, %s)",
                            (country_name, country_code)
                        )
                        country_id = cursor.lastrowid

                    # Check if population data exists for this year
                    cursor.execute(
                        """SELECT data_id FROM Population 
                           WHERE country_id = %s AND year = %s""",
                        (country_id, current_year)
                    )
                    
                    if cursor.fetchone():
                        logger.info(f"Population data already exists for {country_name} in {current_year}")
                    else:
                        # Insert population data
                        cursor.execute(
                            """INSERT INTO Population 
                               (country_id, source_id, year, population)
                               VALUES (%s, %s, %s, %s)""",
                            (country_id, source_id, current_year, population)
                        )
                        logger.info(f"Added population data for {country_name}: {population}")

                    cnx.commit()

        except mysql.connector.Error as err:
            logger.error(f"Database error processing {country_name}: {err}")
            raise
        except KeyError as e:
            logger.error(f"Invalid country data format: {e}")
            raise

def main():
    """Main function to fetch and store population data."""
    try:
        logger.info("Starting population data update...")
        
        # Fetch all country data
        countries_data = PopulationAPI.fetch_countries()
        logger.info(f"Fetched data for {len(countries_data)} countries")

        # Ensure data source exists
        source_id = PopulationAPI.ensure_data_source()
        
        # Process each country
        for country_data in countries_data:
            try:
                country_name = country_data['name']['common']
                logger.info(f"Processing {country_name}...")
                PopulationAPI.process_country(country_data, source_id)
            except Exception as e:
                logger.error(f"Error processing country: {e}")
                continue

        logger.info("Population data update completed")

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()