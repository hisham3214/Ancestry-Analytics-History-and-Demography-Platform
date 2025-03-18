import requests
import mysql.connector
import datetime
import logging
from typing import Dict, List, Any, Tuple, Optional
import time
import concurrent.futures
import os

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
        # Connection pool
        self.connection_pool = None

    def _init_connection_pool(self):
        """Initialize a connection pool"""
        if not self.connection_pool:
            self.connection_pool = mysql.connector.pooling.MySQLConnectionPool(
                pool_name="un_api_pool",
                pool_size=10,
                **self.db_config
            )

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

    def get_connection(self) -> mysql.connector.connection.MySQLConnection:
        """Get a connection from the pool or create a new one"""
        if self.connection_pool:
            return self.connection_pool.get_connection()
        else:
            return mysql.connector.connect(**self.db_config)

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
        Fetch available countries (locations) from the UN API, handling pagination
        """
        all_countries = []
        page = 1
        page_size = 100
        
        logger.info("Fetching countries (locations) from UN API")
        
        while True:
            endpoint = f"locations?pageSize={page_size}&pageNumber={page}"
            try:
                response = self._make_request(endpoint)
                current_data = response.get('data', [])
                
                all_countries.extend(current_data)
                
                total_pages = response.get('pages', 1)
                logger.info(f"Fetched page {page} of {total_pages} for locations")
                
                if page >= total_pages:
                    break
                    
                page += 1
                time.sleep(0.5)  # Add a small delay to be respectful to the API
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to fetch location data, page {page}: {e}")
                break
        
        logger.info(f"Total locations fetched: {len(all_countries)}")
        return all_countries

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
            
            # Prepare batch insert
            values_to_insert = []
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
                    # Add to batch
                    values_to_insert.append((country['name'], iso3))
            
            # Batch insert new countries
            if values_to_insert:
                cursor.executemany("""
                    INSERT INTO Countries (country_name, country_code)
                    VALUES (%s, %s)
                """, values_to_insert)
                
                # Get the IDs of the newly inserted countries
                for i, (name, iso3) in enumerate(values_to_insert):
                    cursor.execute("""
                        SELECT country_id FROM Countries 
                        WHERE country_code = %s
                    """, (iso3,))
                    result = cursor.fetchone()
                    if result:
                        # Find the country ID in the original data
                        for country in countries:
                            if country.get('iso3') == iso3:
                                country_mapping[country['id']] = result[0]
                                break
            
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
                age_specific = False
            elif table_name == 'Death_Rate':
                value_column = 'death_rate'
                sex_specific = False
                age_specific = False
            elif table_name == 'Total_Net_Migration':
                value_column = 'net_migration'
                sex_specific = False
                age_specific = False
            elif table_name == 'Fertility_Rate':
                value_column = 'fertility_rate'
                sex_specific = False
                age_specific = False
            elif table_name == 'Crude_Net_Migration_Rate':
                value_column = 'migration_rate'
                sex_specific = False
                age_specific = False
            elif table_name == "sex_ratio_total_population":
                value_column = 'sex_ratio'
                sex_specific = False
                age_specific = False
            elif table_name == 'sex_ratio_at_birth':
                value_column = 'sex_ratio'
                sex_specific = False
                age_specific = False
            elif table_name == 'median_age':
                value_column = 'age'
                sex_specific = False
                age_specific = False
            elif table_name == 'life_expectancy_at_birth_by_sex':
                value_column = 'life_expectancy'
                sex_specific = True  # We want to capture sex-specific data for this
                age_specific = False
            elif table_name == 'Infant_Mortality_Rate_By_Sex':
                value_column = 'infant_mortality_rate'
                sex_specific = True
                age_specific = False
            elif table_name == 'Under_Five_Mortality_Rate_By_Sex':
                value_column = 'mortality_rate'
                sex_specific = True
                age_specific = False
            elif table_name == 'Population_By_Age_Group':
                value_column = 'population'
                age_specific = True
                sex_specific = True
            else:
                value_column = 'population'
                sex_specific = False
                age_specific = False
            
            # For standard tables, including Population (both sexes)
            if not sex_specific and not age_specific:
                # Prepare batch inserts
                both_sexes_records = []
                population_by_sex_records = []
                
                for record in data:
                    if record.get('variantId') != 4:
                        continue
                    
                    # Insert "both sexes" data into main table
                    if record.get('sexId') == 3:
                        both_sexes_records.append((
                            country_id,
                            self.source_id,
                            record['timeLabel'],
                            record['value'],
                            datetime.now()
                        ))
                    
                    # If this is population data, also prepare sex-specific data
                    if table_name == 'Population' and 'sexId' in record and 'sex' in record:
                        population_by_sex_records.append((
                            country_id,
                            self.source_id,
                            record['timeLabel'],
                            record['sexId'],
                            record['sex'],
                            record['value'],
                            datetime.now()
                        ))
                
                # Batch insert both sexes data
                if both_sexes_records:
                    cursor.executemany(f"""
                        INSERT IGNORE INTO {table_name} 
                        (country_id, source_id, year, {value_column}, last_updated)
                        VALUES (%s, %s, %s, %s, %s)
                    """, both_sexes_records)
                
                # Batch insert sex-specific population data
                if table_name == 'Population' and population_by_sex_records:
                    cursor.executemany("""
                        INSERT IGNORE INTO Population_By_Sex
                        (country_id, source_id, year, sex_id, sex, population, last_updated)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, population_by_sex_records)
                    
            elif table_name == 'Population_By_Age_Group':
                # Prepare batch inserts for age group data
                age_group_records = []
                
                for record in data:
                    if record.get('variantId') != 4:
                        continue
                        
                    # Make sure all required fields exist
                    if ('sexId' not in record or 'sex' not in record or 
                        'ageId' not in record or 'ageLabel' not in record or
                        'ageStart' not in record or 'ageEnd' not in record):
                        continue
                    
                    age_group_records.append((
                        country_id,
                        self.source_id,
                        record['timeLabel'],
                        record['sexId'],
                        record['sex'],
                        record['ageId'],
                        record['ageLabel'],
                        record['ageStart'],
                        record['ageEnd'],
                        record['value'],
                        datetime.now()
                    ))
                
                # Batch insert age group data
                if age_group_records:
                    cursor.executemany("""
                        INSERT IGNORE INTO Population_By_Age_Group
                        (country_id, source_id, year, sex_id, sex, age_group_id, 
                         age_group_label, age_start, age_end, population, last_updated)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, age_group_records)
            else:
                # Prepare batch inserts for sex-specific tables
                sex_specific_records = []
                
                for record in data:
                    if record.get('variantId') != 4:
                        continue
                        
                    # Make sure sexId and sex fields exist
                    if 'sexId' not in record or 'sex' not in record:
                        continue
                        
                    sex_specific_records.append((
                        country_id,
                        self.source_id,
                        record['timeLabel'],
                        record['sexId'],
                        record['sex'],
                        record['value'],
                        datetime.now()
                    ))
                
                # Batch insert sex-specific data
                if sex_specific_records:
                    cursor.executemany(f"""
                        INSERT IGNORE INTO {table_name}
                        (country_id, source_id, year, sex_id, sex, {value_column}, last_updated)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, sex_specific_records)
            
            connection.commit()
        finally:
            cursor.close()

    def process_country_indicator(self, location_id: str, country_id: int, 
                                 indicator_id: str, table_name: str,
                                 start_year: int, end_year: int) -> Tuple[str, str, int]:
        """
        Process a single country-indicator combination.
        Returns a tuple of (location_id, indicator_id, record_count)
        """
        try:
            connection = self.get_connection()
            try:
                logger.info(f"Fetching {table_name} data for location {location_id}...")
                data = self.fetch_demographic_data_for_country(
                    indicator_id, location_id, start_year, end_year
                )
                
                if data:
                    logger.info(f"Inserting {len(data)} records for location {location_id}, indicator {indicator_id}...")
                    self.insert_demographic_data(connection, data, table_name, country_id)
                    return (location_id, indicator_id, len(data))
                return (location_id, indicator_id, 0)
            finally:
                connection.close()
        except Exception as e:
            logger.error(f"Error processing {table_name} data for location {location_id}: {e}")
            return (location_id, indicator_id, 0)

    def filter_countries(self, countries: Dict[str, int], 
                        country_codes: Optional[List[str]] = None) -> Dict[str, int]:
        """
        Filter countries by country code if a list is provided.
        """
        if not country_codes:
            return countries
            
        # Get a connection from the pool
        connection = self.get_connection()
        try:
            # Get country IDs for the requested country codes
            cursor = connection.cursor(buffered=True)
            
            # Convert country codes to uppercase for consistent comparison
            country_codes = [code.upper() for code in country_codes]
            
            # Build the SQL placeholders for the IN clause
            placeholders = ', '.join(['%s'] * len(country_codes))
            
            # Query to get country_id and location_id mapping
            cursor.execute(f"""
                SELECT c.country_id, c.country_code 
                FROM Countries c
                WHERE c.country_code IN ({placeholders})
            """, country_codes)
            
            # Get the results
            filtered_countries = {}
            for row in cursor.fetchall():
                country_id, country_code = row
                
                # Find the location_id for this country_id
                for location_id, c_id in countries.items():
                    if c_id == country_id:
                        filtered_countries[location_id] = country_id
                        break
            
            cursor.close()
            return filtered_countries
        finally:
            connection.close()

    def populate_database(self, start_year: int = 1950, end_year: int = 2025,
                         country_codes: Optional[List[str]] = None,
                         indicators_to_process: Optional[List[str]] = None,
                         max_workers: int = 5):
        """
        Main function to populate the database with demographic data.
        
        Args:
            start_year: Starting year for data collection
            end_year: Ending year for data collection
            country_codes: Optional list of ISO3 country codes to process (e.g., ["USA", "CAN"])
            indicators_to_process: Optional list of indicator IDs to process (e.g., ["49", "61"])
            max_workers: Maximum number of concurrent workers
        """
        # Indicator IDs for UN Population API (as per their docs)
        all_indicators = {
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
            '46': 'Population_By_Age_Group',  # Population by 5-year age groups and sex
        }
        
        # Filter indicators if specified
        indicators = {}
        if indicators_to_process:
            for indicator_id in indicators_to_process:
                if indicator_id in all_indicators:
                    indicators[indicator_id] = all_indicators[indicator_id]
        else:
            indicators = all_indicators

        try:
            # Initialize the connection pool
            self._init_connection_pool()
            
            # Get a connection for initial setup
            connection = self.get_connection()
            try:
                self.source_id = self.setup_data_source(connection)
                
                if not self.source_id:
                    raise ValueError("Failed to setup data source")

                # Insert countries and build the mapping from UN location_id -> country_id
                logger.info("Inserting countries...")
                country_mapping = self.insert_countries(connection)
                
                # Filter countries if specific ones were requested
                if country_codes:
                    country_mapping = self.filter_countries(country_mapping, country_codes)
                
                logger.info(f"Processing data for {len(country_mapping)} countries and {len(indicators)} indicators...")
            finally:
                connection.close()

            # Prepare tasks for thread pool
            tasks = []
            for location_id, country_id in country_mapping.items():
                for indicator_id, table_name in indicators.items():
                    tasks.append((location_id, country_id, indicator_id, table_name))
            
            # Process tasks in parallel using a thread pool
            results = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []
                for location_id, country_id, indicator_id, table_name in tasks:
                    # Submit each task to the thread pool
                    future = executor.submit(
                        self.process_country_indicator,
                        location_id, country_id, indicator_id, table_name,
                        start_year, end_year
                    )
                    futures.append((future, location_id, indicator_id))
                
                # Collect results as tasks complete
                for future, location_id, indicator_id in futures:
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        logger.error(f"Task for location {location_id}, indicator {indicator_id} failed: {e}")
            
            # Log summary of processed data
            total_records = sum(count for _, _, count in results)
            logger.info(f"Database population complete. Processed {total_records} records across {len(results)} tasks.")

        except Exception as e:
            logger.error(f"Error in database population: {e}")
            raise

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
    
    # Example 1: Process everything (slow)
    # un_api.populate_database()
    
    # Example 2: Process only specific countries and indicators (faster)
    #un_api.populate_database(
    #    country_codes=["USA", "GBR", "CAN", "FRA", "DEU", "JPN", "CHN", "IND", "BRA", "RUS"],
    #    indicators_to_process=["49", "61", "46"],  # Population, Life expectancy, Population by age
    #    max_workers=5  # Adjust based on your system capabilities
    #)
    # Example 3: Process everything in parallel (faster)
    un_api.populate_database(max_workers=10)