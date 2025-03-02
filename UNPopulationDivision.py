import requests
import mysql.connector
from datetime import datetime
import logging
from typing import Dict, List, Any
import time

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class UNPopulationAPI:
    def __init__(self, db_config: Dict[str, str]):
        """
        Initialize the UN Population API client
        """
        self.base_url = "https://population.un.org/dataportalapi/api/v1"
        self.db_config = db_config
        self.source_id = None
        self.headers = {
            'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1bmlxdWVfbmFtZSI6ImhtYTE0NkBtYWlsLmF1Yi5lZHUiLCJuYmYiOjE3Mzc1NTc1MDMsImV4cCI6MTc2OTA5MzUwMywiaWF0IjoxNzM3NTU3NTAzLCJpc3MiOiJkb3RuZXQtdXNlci1qd3RzIiwiYXVkIjoiZGF0YS1wb3J0YWwtYXBpIn0.eOqaGLUBcOWfjUi90UrV9x335UfrsuGGmobGQREwxQg'
        }

    def _make_request(self, endpoint: str, retry_count: int = 3) -> Dict:
        """
        Make an authenticated request to the UN API with retries
        """
        url = f"{self.base_url}/{endpoint}"
        for attempt in range(retry_count):
            try:
                response = requests.get(url, headers=self.headers)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                if attempt == retry_count - 1:  # Last attempt
                    logger.error(f"API request failed for endpoint {endpoint}: {str(e)}")
                    raise
                time.sleep(1 * (attempt + 1))  # Simple backoff
                continue

    def fetch_demographic_data_for_country(self, indicator_id: int, location_id: str, 
                                     start_year: int, end_year: int) -> List[Dict[str, Any]]:
        """
        Fetch demographic data for a specific indicator and country, handling pagination
        """
        all_data = []
        page = 1
        page_size = 1000
        
        logger.info(f"Fetching data for indicator {indicator_id}, location {location_id}")
        
        while True:
            endpoint = (f"data/indicators/{indicator_id}/locations/{location_id}"
                    f"/start/{start_year}/end/{end_year}"
                    f"?format=json&pageSize={page_size}&pageNumber={page}"
                    f"&pagingInHeader=false")
            try:
                response = self._make_request(endpoint)
                current_data = response.get('data', [])
                
                all_data.extend(current_data)
                
                total_pages = response.get('pages', 1)
                logger.info(f"Fetched page {page} of {total_pages} for location {location_id}")
                
                if page >= total_pages:
                    break
                    
                page += 1
                time.sleep(0.5)
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to fetch data for indicator {indicator_id}, "
                            f"location {location_id}, page {page}: {e}")
                break
        
        logger.info(f"Total records fetched for location {location_id}: {len(all_data)}")
        return all_data

    def setup_data_source(self, connection: mysql.connector.connection.MySQLConnection) -> int:
        """Insert or retrieve UN Population Division as a data source"""
        cursor = connection.cursor(buffered=True)
        try:
            # Check if the data source is already in the table
            cursor.execute("""
                SELECT source_id FROM Data_Sources 
                WHERE name = 'United Nations Population Division'
            """)
            result = cursor.fetchone()
            
            if result:
                return result[0]
            
            # If not, insert it
            cursor.execute("""
                INSERT INTO Data_Sources (name, website) 
                VALUES ('United Nations Population Division', 'https://population.un.org')
            """)
            connection.commit()
            
            # Retrieve the newly inserted source_id
            cursor.execute("""
                SELECT source_id FROM Data_Sources 
                WHERE name = 'United Nations Population Division'
            """)
            result = cursor.fetchone()
            return result[0] if result else None
            
        finally:
            cursor.close()

    def fetch_countries(self) -> List[Dict[str, Any]]:
        """
        Fetch available countries (locations) from the UN API
        """
        response = self._make_request("locations")
        return response.get('data', [])

    def insert_countries(self, connection: mysql.connector.connection.MySQLConnection) -> Dict[str, int]:
        """
        Insert countries and return a mapping of UN location_id -> country_id.
        
        This implementation ensures each ISO3 is only processed once
        by using a set to track seen ISO3 codes.
        """
        countries = self.fetch_countries()
        cursor = connection.cursor(buffered=True)
        country_mapping = {}
        
        try:
            # First get existing country codes and IDs from the DB
            cursor.execute("SELECT country_id, country_code FROM Countries")
            existing_countries = {row[1]: row[0] for row in cursor.fetchall()}
            
            # We'll keep track of iso3 codes we've seen
            seen_iso3 = set()

            for country in countries:
                # If there's no iso3 code, skip
                if 'iso3' not in country or not country['iso3']:
                    continue
                iso3 = country['iso3']

                # If we've already handled this iso3, skip any duplicates
                if iso3 in seen_iso3:
                    continue
                seen_iso3.add(iso3)
                
                # If this iso3 already exists in DB, just map that location_id to the existing country
                if iso3 in existing_countries:
                    country_mapping[country['id']] = existing_countries[iso3]
                else:
                    # Otherwise, insert into Countries
                    cursor.execute("""
                        INSERT INTO Countries (country_name, country_code)
                        VALUES (%s, %s)
                    """, (country['name'], iso3))
                    country_mapping[country['id']] = cursor.lastrowid
                    
            connection.commit()
            return country_mapping
            
        finally:
            cursor.close()

    def insert_demographic_data(self, connection: mysql.connector.connection.MySQLConnection, 
                            data: List[Dict[str, Any]], table_name: str,
                            country_id: int):
        """
        Insert demographic data for a specific country and table
        """
        if not data:
            return

        cursor = connection.cursor(buffered=True)
        try:
            # Determine which columns to use based on the table name
            if table_name == 'Birth_Rate':
                value_column = 'birth_rate'
                sex_specific = False
            elif table_name == 'Death_Rate':
                value_column = 'death_rate'
                sex_specific = False
            elif table_name == 'Total_Net_Migration':
                value_column = 'net_migration'
                sex_specific = False
            elif table_name == 'Fertility_Rate':
                value_column = 'fertility_rate'
                sex_specific = False
            elif table_name == 'Crude_Net_Migration_Rate':
                value_column = 'migration_rate'
                sex_specific = False
            elif table_name == "sex_ratio_total_population":
                value_column = 'sex_ratio'
                sex_specific = False
            elif table_name == 'sex_ratio_at_birth':
                value_column = 'sex_ratio'
                sex_specific = False
            elif table_name == 'median_age':
                value_column = 'age'
                sex_specific = False
            elif table_name == 'life_expectancy_at_birth_by_sex':
                value_column = 'life_expectancy'
                sex_specific = True  # We want to capture sex-specific data for this
            elif table_name == 'Infant_Mortality_Rate_By_Sex':
                value_column = 'infant_mortality_rate'
                sex_specific = True
            elif table_name == 'Under_Five_Mortality_Rate_By_Sex':
                value_column = 'mortality_rate'
                sex_specific = True
            else:
                value_column = 'population'
                sex_specific = False
            
            # For most tables, we use the existing logic for "both sexes"
            if not sex_specific:
                insert_sql = f"""
                    INSERT IGNORE INTO {table_name} 
                    (country_id, source_id, year, {value_column}, last_updated)
                    VALUES (%s, %s, %s, %s, %s)
                """

                # Insert data only for "both sexes" (sexId = 3)
                for record in data:
                    if record.get('sexId') != 3:
                        continue
                    if record.get('variantId') != 4:
                        continue
                        
                    cursor.execute(insert_sql, (
                        country_id,
                        self.source_id,
                        record['timeLabel'],
                        record['value'],
                        datetime.now()
                    ))
            else:
                # For sex-specific tables (currently only life expectancy), use a different SQL
                # that includes the sex column
                insert_sql = f"""
                    INSERT IGNORE INTO {table_name}_by_sex
                    (country_id, source_id, year, sex_id, sex, {value_column}, last_updated)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """

                # Process all records regardless of sexId
                for record in data:
                    if record.get('variantId') != 4:
                        continue
                        
                    # Make sure sexId and sex fields exist
                    if 'sexId' not in record or 'sex' not in record:
                        continue
                        
                    cursor.execute(insert_sql, (
                        country_id,
                        self.source_id,
                        record['timeLabel'],
                        record['sexId'],
                        record['sex'],
                        record['value'],
                        datetime.now()
                    ))
            
            connection.commit()
        finally:
            cursor.close()

    def populate_database(self, start_year: int = 1950, end_year: int = 2025):
        """
        Main function to populate the database with demographic data.
        """
        # Indicator IDs for UN Population API (as per their docs)
        indicators = {
            '66': 'Crude_Net_Migration_Rate',  # Net migration
            '65': 'Total_Net_Migration',  # Net migration
            '19': 'Fertility_Rate',  # Total fertility rate
            '59': 'Death_Rate',  # Crude death rate (deaths per 1,000 population)
            '55': 'Birth_Rate',  # Crude birth rate (births per 1,000 population)
            '72': 'sex_ratio_total_population', # Number of males for every 100 females
            '58': 'sex_ratio_at_birth', # Number of male births per female birth
            '67': 'median_age', # Median age of the population
            '61': 'life_expectancy_at_birth_by_sex',  # Life expectancy at birth
            '49': 'Population',  # Total population
            '22': 'Infant_Mortality_Rate_By_Sex',  # Infant mortality rate
            '24': 'Under_Five_Mortality_Rate_By_Sex', # Under-5 mortality rate
        }

        try:
            connection = self.connect_db()
            self.source_id = self.setup_data_source(connection)
            
            if not self.source_id:
                raise ValueError("Failed to setup data source")

            # Insert countries and build the mapping from UN location_id -> DB country_id
            logger.info("Inserting countries...")
            country_mapping = self.insert_countries(connection)
            logger.info(f"Processing data for {len(country_mapping)} countries...")

            # For each indicator, fetch and insert data for every country
            for indicator_id, table_name in indicators.items():
                logger.info(f"Processing {table_name} data...")

                for location_id, country_id in country_mapping.items():
                    try:
                        logger.info(f"Fetching {table_name} data for location {location_id}...")
                        data = self.fetch_demographic_data_for_country(
                            indicator_id, location_id, start_year, end_year
                        )
                        if data:
                            logger.info(f"Inserting {len(data)} records for location {location_id}...")
                            self.insert_demographic_data(connection, data, table_name, country_id)
                        # Sleep a bit to respect rate limits
                        time.sleep(0.5)
                    except Exception as e:
                        logger.error(f"Error processing {table_name} data for location {location_id}: {e}")
                        continue

        except Exception as e:
            logger.error(f"Error in database population: {e}")
            raise
        finally:
            if 'connection' in locals():
                connection.close()

    def connect_db(self) -> mysql.connector.connection.MySQLConnection:
        """
        Establish a database connection
        """
        try:
            connection = mysql.connector.connect(**self.db_config)
            return connection
        except mysql.connector.Error as err:
            logger.error(f"Database connection failed: {err}")
            raise

# Usage example:
if __name__ == "__main__":
    db_config = {
        'user': 'root',
        'password': 'LZ#amhe!32',
        'host': '127.0.0.1',
        'database': 'fyp',
        'raise_on_warnings': True
    }

    un_api = UNPopulationAPI(db_config)
    un_api.populate_database()