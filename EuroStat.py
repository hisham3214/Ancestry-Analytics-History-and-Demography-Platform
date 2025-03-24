import requests
import mysql.connector
from mysql.connector import errorcode
from datetime import datetime
import logging
import time
from typing import Dict, List, Any

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EurostatAPI:
    def __init__(self, db_config: Dict[str, str]):
        """
        Initialize the Eurostat API client
        """
        self.base_url = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
        self.db_config = db_config
        self.source_id = None
        
        # Map of two-letter to three-letter country codes
        self.country_code_map = {
            "AF": "AFG", "AL": "ALB", "DZ": "DZA", "AS": "ASM", "AD": "AND", 
            "AO": "AGO", "AI": "AIA", "AQ": "ATA", "AG": "ATG", "AR": "ARG", 
            "AM": "ARM", "AW": "ABW", "AU": "AUS", "AT": "AUT", "AZ": "AZE", 
            "BS": "BHS", "BH": "BHR", "BD": "BGD", "BB": "BRB", "BY": "BLR", 
            "BE": "BEL", "BZ": "BLZ", "BJ": "BEN", "BM": "BMU", "BT": "BTN", 
            "BO": "BOL", "BQ": "BES", "BA": "BIH", "BW": "BWA", "BV": "BVT", 
            "BR": "BRA", "IO": "IOT", "BN": "BRN", "BG": "BGR", "BF": "BFA", 
            "BI": "BDI", "CV": "CPV", "KH": "KHM", "CM": "CMR", "CA": "CAN", 
            "KY": "CYM", "CF": "CAF", "TD": "TCD", "CL": "CHL", "CN": "CHN", 
            "CX": "CXR", "CC": "CCK", "CO": "COL", "KM": "COM", "CD": "COD", 
            "CG": "COG", "CK": "COK", "CR": "CRI", "HR": "HRV", "CU": "CUB", 
            "CW": "CUW", "CY": "CYP", "CZ": "CZE", "CI": "CIV", "DK": "DNK", 
            "DJ": "DJI", "DM": "DMA", "DO": "DOM", "EC": "ECU", "EG": "EGY", 
            "SM": "SMR", "SV": "SLV", "GQ": "GNQ", "ER": "ERI", "EE": "EST", 
            "SZ": "SWZ", "ET": "ETH", "FI": "FIN", "FR": "FRA", "GE": "GEO", 
            "DE": "DEU", "GH": "GHA", "GR": "GRC", "HU": "HUN", "IS": "ISL", 
            "IN": "IND", "ID": "IDN", "IR": "IRN", "IQ": "IRQ", "IE": "IRL", 
            "IL": "ISR", "IT": "ITA", "JP": "JPN", "JO": "JOR", "KZ": "KAZ", 
            "KE": "KEN", "KW": "KWT", "LV": "LVA", "LB": "LBN", "LT": "LTU", 
            "LU": "LUX", "MT": "MLT", "MX": "MEX", "MD": "MDA", "MC": "MCO", 
            "ME": "MNE", "MA": "MAR", "NL": "NLD", "NZ": "NZL", "NO": "NOR", 
            "PK": "PAK", "PL": "POL", "PT": "PRT", "QA": "QAT", "RO": "ROU", 
            "RU": "RUS", "SA": "SAU", "RS": "SRB", "SG": "SGP", "SK": "SVK", 
            "SI": "SVN", "ZA": "ZAF", "KR": "KOR", "ES": "ESP", "SE": "SWE", 
            "CH": "CHE", "TH": "THA", "TR": "TUR", "UA": "UKR", "AE": "ARE", 
            "GB": "GBR", "UK": "GBR", "US": "USA", "VN": "VNM", "YE": "YEM", 
            "ZM": "ZMB", "ZW": "ZWE", "XK": "XKX", "MK": "MKD", "LI": "LIE", 
            "EL": "GRC"
        }

    def _make_request(self, endpoint: str, params: Dict, retry_count: int = 3) -> Dict:
        """
        Make a request to the Eurostat API with retries
        """
        url = f"{self.base_url}/{endpoint}"
        for attempt in range(retry_count):
            try:
                response = requests.get(url, params=params)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                if attempt == retry_count - 1:  # Last attempt
                    logger.error(f"API request failed for endpoint {endpoint}: {str(e)}")
                    raise
                time.sleep(1 * (attempt + 1))  # Simple backoff
                continue

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

    def setup_data_source(self, connection: mysql.connector.connection.MySQLConnection) -> int:
        """Insert or retrieve Eurostat as a data source"""
        cursor = connection.cursor(buffered=True)
        try:
            # Check if the data source is already in the table
            cursor.execute("""
                SELECT source_id FROM Data_Sources 
                WHERE name = 'Eurostat'
            """)
            result = cursor.fetchone()
            
            if result:
                return result[0]
            
            # If not, insert it
            cursor.execute("""
                INSERT INTO Data_Sources (name, website) 
                VALUES ('Eurostat', 'https://ec.europa.eu/eurostat')
            """)
            connection.commit()
            
            # Retrieve the newly inserted source_id
            cursor.execute("""
                SELECT source_id FROM Data_Sources 
                WHERE name = 'Eurostat'
            """)
            result = cursor.fetchone()
            return result[0] if result else None
            
        finally:
            cursor.close()

    def get_available_countries(self) -> List[Dict[str, Any]]:
        """
        Get available countries from the Eurostat API demo_gind table
        """
        try:
            # Make a request to get a single indicator to extract the list of countries
            params = {
                'format': 'JSON',
                'indic_de': 'GBIRTHRT',
            }
            response = self._make_request('demo_gind', params)
            
            countries = []
            geo_index = response['dimension']['geo']['category']['index']
            geo_label = response['dimension']['geo']['category']['label']
            
            for country_code, index in geo_index.items():
                # Skip aggregate regions like EU27, EU28, etc.
                if '_' in country_code or len(country_code) > 2:
                    continue
                    
                countries.append({
                    'id': country_code,
                    'name': geo_label[country_code],
                    'iso3': self.country_code_map.get(country_code, country_code)
                })
                
            return countries
        except Exception as e:
            logger.error(f"Error fetching countries: {e}")
            return []

    def get_or_insert_country(self, cursor, country_name: str, country_code: str) -> int:
        """
        Returns the country_id from the Countries table if it exists,
        otherwise inserts a new record and returns its ID.
        """
        # Convert 2-letter code to 3-letter if possible
        iso3 = self.country_code_map.get(country_code, country_code)
        
        query = """
            SELECT country_id
            FROM Countries
            WHERE country_code = %s
            LIMIT 1
        """
        cursor.execute(query, (iso3,))
        row = cursor.fetchone()
        if row:
            return row[0]

        # Insert new country
        insert_sql = """
            INSERT INTO Countries (country_name, country_code)
            VALUES (%s, %s)
        """
        cursor.execute(insert_sql, (country_name, iso3))
        return cursor.lastrowid

    def fetch_demographic_data(self, indicator_code: str, country_code: str) -> Dict:
        """
        Fetch demographic data for a specific indicator and country.
        
        Args:
            indicator_code: The Eurostat indicator code (e.g., 'GBIRTHRT')
            country_code: The country code (e.g., 'DE')
            
        Returns:
            Dictionary with the data response
        """
        # For most indicators, they're in the demo_gind table
        endpoint = 'demo_gind'
        
        # For total population (AVG_POP), we need to use demo_pjan
        if indicator_code == 'AVG_POP':
            endpoint = 'demo_pjan'
            params = {
                'format': 'JSON',
                'unit': 'NR',
                'sex': 'T',
                'age': 'TOTAL',
                'geo': country_code
            }
        else:
            params = {
                'format': 'JSON',
                'indic_de': indicator_code,
                'geo': country_code
            }
        
        try:
            logger.info(f"Fetching {indicator_code} data for {country_code}")
            response = self._make_request(endpoint, params)
            return response
        except Exception as e:
            logger.error(f"Error fetching {indicator_code} data for {country_code}: {e}")
            return None

    def insert_demographic_data(self, connection: mysql.connector.connection.MySQLConnection,
                              data: Dict, indicator_code: str, country_id: int):
        """
        Insert demographic data for a specific country and indicator
        """
        if not data or 'value' not in data or not data['value']:
            logger.warning(f"No data to insert for indicator {indicator_code}, country_id {country_id}")
            return

        cursor = connection.cursor(buffered=True)
        try:
            # Determine which table and column to use based on the indicator code
            if indicator_code == 'GBIRTHRT':
                table_name = 'Birth_Rate'
                value_column = 'birth_rate'
            elif indicator_code == 'GDEATHRT':
                table_name = 'Death_Rate'
                value_column = 'death_rate'
            elif indicator_code == 'CNMIGRATRT':
                table_name = 'Crude_Net_Migration_Rate'
                value_column = 'migration_rate'
            elif indicator_code == 'AVG_POP':
                table_name = 'Population'
                value_column = 'population'
            elif indicator_code == 'GROWRT':
                table_name = 'Population_Growth_Rate'
                value_column = 'growth_rate'
            elif indicator_code == 'NATGROWRT':
                table_name = 'Natural_Growth_Rate'
                value_column = 'growth_rate'
            elif indicator_code == 'NATGROW':
                table_name = 'Natural_Change'
                value_column = 'change_value'
            elif indicator_code == 'NPOPGROW':
                table_name = 'Net_Population_Change'
                value_column = 'change_value'
            elif indicator_code == 'CNMIGRAT':
                table_name = 'Total_Net_Migration'
                value_column = 'net_migration'
            else:
                logger.warning(f"Unknown indicator code: {indicator_code}")
                return
                
            # Extract time index mapping
            time_index = data['dimension']['time']['category']['index']
            
            # Insert data for each time period
            for year_str, time_idx in time_index.items():
                value = data['value'].get(str(time_idx))
                if value is None:
                    continue
                    
                year_int = int(year_str)
                
                # Check if the entry already exists
                check_sql = f"""
                    SELECT 1 FROM {table_name}
                    WHERE country_id = %s AND source_id = %s AND year = %s
                    LIMIT 1
                """
                cursor.execute(check_sql, (country_id, self.source_id, year_int))
                if cursor.fetchone():
                    # Update existing record
                    update_sql = f"""
                        UPDATE {table_name}
                        SET {value_column} = %s, last_updated = %s
                        WHERE country_id = %s AND source_id = %s AND year = %s
                    """
                    cursor.execute(update_sql, (value, datetime.now(), country_id, self.source_id, year_int))
                else:
                    # Insert new record
                    insert_sql = f"""
                        INSERT INTO {table_name} (country_id, source_id, year, {value_column}, last_updated)
                        VALUES (%s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_sql, (country_id, self.source_id, year_int, value, datetime.now()))
            
            connection.commit()
            logger.info(f"Inserted/updated data for indicator {indicator_code}, country_id {country_id}")
        except Exception as e:
            logger.error(f"Error inserting data for indicator {indicator_code}, country_id {country_id}: {e}")
            connection.rollback()
        finally:
            cursor.close()

    def populate_database(self, selected_indicators=None):
        """
        Main function to populate the database with demographic data from Eurostat.
        
        Args:
            selected_indicators: Optional list of indicator codes to fetch.
                                If None, all available indicators will be fetched.
        """
        # All available indicators
        all_indicators = {
            'GBIRTHRT': 'Crude birth rate',
            'GDEATHRT': 'Crude death rate',
            'CNMIGRATRT': 'Crude rate of net migration plus statistical adjustment',
            'AVG_POP': 'Average population',  # Total population (yearly average)
            'GROWRT': 'Population change',
            'NATGROWRT': 'Natural change of population',
            'NATGROW': 'Natural change',
            'NPOPGROW': 'Net change',
            'CNMIGRAT': 'Net migration plus statistical adjustment'
        }
        
        # If specific indicators are requested, filter the dictionary
        if selected_indicators:
            indicators = {k: v for k, v in all_indicators.items() if k in selected_indicators}
        else:
            indicators = all_indicators
        
        try:
            connection = self.connect_db()
            self.source_id = self.setup_data_source(connection)
            
            if not self.source_id:
                raise ValueError("Failed to setup data source")
                
            # Get the list of available countries
            countries = self.get_available_countries()
            logger.info(f"Processing data for {len(countries)} countries")
            
            # For each indicator, fetch and insert data for every country
            for indicator_code, indicator_name in indicators.items():
                logger.info(f"Processing {indicator_name} data...")
                
                for country in countries:
                    country_code = country['id']
                    country_name = country['name']
                    
                    try:
                        # Get or insert the country
                        cursor = connection.cursor(buffered=True)
                        country_id = self.get_or_insert_country(cursor, country_name, country_code)
                        cursor.close()
                        
                        # Fetch demographic data
                        data = self.fetch_demographic_data(indicator_code, country_code)
                        
                        if data:
                            # Insert demographic data
                            self.insert_demographic_data(connection, data, indicator_code, country_id)
                            
                        # Sleep a bit to respect rate limits
                        time.sleep(0.5)
                    except Exception as e:
                        logger.error(f"Error processing {indicator_name} data for {country_name}: {e}")
                        continue
                        
        except Exception as e:
            logger.error(f"Error in database population: {e}")
            raise
        finally:
            if 'connection' in locals():
                connection.close()


# Usage example:
if __name__ == "__main__":
    db_config = {
        'user': 'root',
        'password': 'LZ#amhe!32',
        'host': '127.0.0.1',
        'database': 'fyp1',
        'raise_on_warnings': True
    }

    # Define which indicators to fetch
    # Available options:
    # - 'GBIRTHRT': Crude birth rate
    # - 'GDEATHRT': Crude death rate
    # - 'CNMIGRATRT': Crude rate of net migration plus statistical adjustment
    # - 'AVG_POP': Average population (total population)
    # - 'GROWRT': Population change
    # - 'NATGROWRT': Natural change of population
    # - 'NATGROW': Natural change
    # - 'NPOPGROW': Net change
    # - 'CNMIGRAT': Net migration plus statistical adjustment
    
    selected_indicators = [
        'AVG_POP'        # Total population
    ]
    
    eurostat_api = EurostatAPI(db_config)
    eurostat_api.populate_database(selected_indicators)