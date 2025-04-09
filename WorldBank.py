import requests
import mysql.connector
from datetime import datetime
import time


class WorldBankDataFetcher:
    def __init__(self, db_config):
        """
        Initialize the data fetcher with database configuration
        db_config should be a dictionary with: host, user, password, database
        """
        self.db_config = db_config
        self.base_url = "http://api.worldbank.org/v2"
        self.all_indicators = {
            'birth_rate': 'SP.DYN.CBRT.IN',  # Birth rate, crude (per 1,000 people)
            'death_rate': 'SP.DYN.CDRT.IN',  # Death rate, crude (per 1,000 people)
            'population': 'SP.POP.TOTL',      # Population, total
            'net_migration': 'SM.POP.NETM',   # Net migration
            'fertility_rate': 'SP.DYN.TFRT.IN',  # Fertility rate, total (births per woman)
            'life_expectancy': 'SP.DYN.LE00.IN',  # Life expectancy at birth, total (years)
            'infant_mortality': 'SP.DYN.IMRT.IN',  # Mortality rate, infant (per 1,000 live births)
            'gdp_growth': 'NY.GDP.MKTP.KD.ZG',  # GDP growth (annual %)
            'gdp_per_capita': 'NY.GDP.PCAP.CD'  # GDP per capita (current US$)
        }
        # Default to all indicators if not specified later
        self.selected_indicators = self.all_indicators.copy()
        self.excluded_countries = []
        
    def connect_db(self):
        """Establish database connection"""
        return mysql.connector.connect(**self.db_config)

    def fetch_countries(self):
        """
        Fetch list of countries from World Bank API with pagination support
        """
        countries = []
        page = 1
        per_page = 100
        more_pages = True
        
        while more_pages:
            url = f"{self.base_url}/country?format=json&page={page}&per_page={per_page}"
            response = requests.get(url)
            
            try:
                # Check if there's pagination info
                if isinstance(response.json(), list) and len(response.json()) > 1:
                    # Get pagination info from first element
                    pagination = response.json()[0]
                    data = response.json()[1]  # Skip pagination info
                    
                    # Process countries
                    for country in data:
                        if country['region']['id'] != "NA":  # Skip aggregates/regions
                            countries.append({
                                'name': country['name'],
                                'code': country['id'],
                                'iso3': country.get('iso3code', '')
                            })
                    
                    # Check if we need to fetch more pages
                    if pagination.get('page', 1) < pagination.get('pages', 1):
                        page += 1
                    else:
                        more_pages = False
                else:
                    more_pages = False
            except Exception as e:
                print(f"Error parsing API response: {e}")
                more_pages = False
                
            # Rate limiting
            time.sleep(0.5)
            
        return countries

    def set_selected_indicators(self, indicator_list=None):
        """
        Set which indicators to fetch
        indicator_list: list of indicator keys to include, or None for all
        """
        if indicator_list is None:
            # Reset to all indicators
            self.selected_indicators = self.all_indicators.copy()
        else:
            # Filter to only selected indicators
            self.selected_indicators = {k: v for k, v in self.all_indicators.items() 
                                      if k in indicator_list}
        return self.selected_indicators

    def set_excluded_countries(self, country_codes=None):
        """
        Set which countries to exclude from fetching
        country_codes: list of ISO country codes to exclude, or None to reset
        """
        if country_codes is None:
            self.excluded_countries = []
        else:
            self.excluded_countries = country_codes
        return self.excluded_countries

    def insert_countries(self, countries):
        """Insert countries into the database"""
        conn = self.connect_db()
        cursor = conn.cursor()
        
        # Get existing countries to avoid duplicates
        cursor.execute("SELECT country_code FROM Countries")
        existing_codes = [row[0] for row in cursor.fetchall()]
        
        count = 0
        for country in countries:
            if country['code'] not in existing_codes:
                sql = "INSERT INTO Countries (country_name, country_code) VALUES (%s, %s)"
                cursor.execute(sql, (country['name'], country['code']))
                count += 1
        
        print(f"Inserted {count} new countries")
        conn.commit()
        cursor.close()
        conn.close()

    def insert_data_source(self):
        """Insert World Bank as data source or get existing ID"""
        conn = self.connect_db()
        cursor = conn.cursor()
        
        # Check if World Bank source already exists
        cursor.execute("SELECT source_id FROM Data_Sources WHERE name = 'World Bank'")
        result = cursor.fetchone()
        
        if result:
            source_id = result[0]
        else:
            sql = "INSERT INTO Data_Sources (name, website) VALUES (%s, %s)"
            cursor.execute(sql, ('World Bank', 'https://data.worldbank.org'))
            source_id = cursor.lastrowid
        
        conn.commit()
        cursor.close()
        conn.close()
        return source_id

    def fetch_indicator_data(self, country_code, indicator, start_year=1900, end_year=2025):
        """Fetch specific indicator data for a country with pagination support"""
        all_data = []
        page = 1
        per_page = 100
        more_pages = True
        
        while more_pages:
            url = f"{self.base_url}/country/{country_code}/indicator/{indicator}"
            params = {
                'format': 'json',
                'date': f"{start_year}:{end_year}",
                'page': page,
                'per_page': per_page
            }
            
            try:
                response = requests.get(url, params=params)
                json_data = response.json()
                
                # Check if we have valid data with pagination
                if isinstance(json_data, list) and len(json_data) > 1:
                    pagination = json_data[0]
                    data = json_data[1]
                    
                    if data:
                        all_data.extend(data)
                    
                    # Check if we need to fetch more pages
                    if pagination.get('page', 1) < pagination.get('pages', 1):
                        page += 1
                    else:
                        more_pages = False
                else:
                    more_pages = False
            except Exception as e:
                print(f"Error fetching data for {country_code}, indicator {indicator}: {e}")
                more_pages = False
            
            # Rate limiting
            time.sleep(0.5)
        
        return all_data

    def get_table_name_for_indicator(self, indicator_key):
        """Convert indicator key to corresponding table name"""
        table_mapping = {
            'birth_rate': 'Birth_Rate',
            'death_rate': 'Death_Rate',
            'population': 'Population',
            'net_migration': 'Total_Net_Migration',
            'fertility_rate': 'Fertility_Rate',
            'life_expectancy': 'Life_Expectancy',
            'infant_mortality': 'Infant_Mortality_Rate',
            'gdp_growth': 'GDP_Growth',
            'gdp_per_capita': 'GDP_Per_Capita'
        }
        return table_mapping.get(indicator_key)

    def populate_indicator_table(self, table_name, country_id, source_id, indicator_data):
        """Insert indicator data into specified table"""
        if not indicator_data:
            print(f"No data available for table {table_name}, country {country_id}")
            return 0
            
        conn = self.connect_db()
        cursor = conn.cursor(buffered=True)
        
        # Determine which column to use based on the table name
        value_column_mapping = {
            'Population': 'population',
            'Birth_Rate': 'birth_rate',
            'Death_Rate': 'death_rate',
            'Total_Net_Migration': 'net_migration',
            'Fertility_Rate': 'fertility_rate',
            'Life_Expectancy': 'life_expectancy',
            'Infant_Mortality_Rate': 'infant_mortality_rate',
            'GDP_Growth': 'gdp_growth',
            'GDP_Per_Capita': 'gdp_per_capita'
        }
        
        value_column = value_column_mapping.get(table_name, 'value')
        
        # Check if table exists
        try:
            cursor.execute(f"DESCRIBE {table_name}")
            # Consume the result to avoid "Unread result found" error
            cursor.fetchall()
        except mysql.connector.Error:
            print(f"Table {table_name} does not exist. Skipping data for this indicator.")
            return 0
        
        # Insert data
        records_inserted = 0
        
        # Prepare all records to insert in batch
        batch_data = []
        for entry in indicator_data:
            if entry['value'] is not None:
                try:
                    batch_data.append((
                        country_id, 
                        source_id, 
                        entry['date'], 
                        float(entry['value']), 
                        datetime.now()
                    ))
                except Exception as e:
                    print(f"Error preparing data: {e}")
        
        # Process in smaller batches to avoid memory issues
        batch_size = 50
        for i in range(0, len(batch_data), batch_size):
            batch_chunk = batch_data[i:i+batch_size]
            try:
                sql = f"""
                INSERT INTO {table_name} 
                (country_id, source_id, year, {value_column}, last_updated)
                VALUES (%s, %s, %s, %s, %s)
                AS new_values
                ON DUPLICATE KEY UPDATE
                {value_column} = new_values.{value_column},
                last_updated = new_values.last_updated
                """
                cursor.executemany(sql, batch_chunk)
                # Commit after each batch
                conn.commit()
                records_inserted += len(batch_chunk)
            except Exception as e:
                print(f"Error inserting batch data: {e}")
        
        cursor.close()
        conn.close()
        
        return records_inserted

    def fetch_and_store_all_data(self, start_year=1900, end_year=2025):
        """Main function to orchestrate the entire data fetching and storing process"""
        # Insert World Bank as data source
        source_id = self.insert_data_source()
        print(f"Using source ID: {source_id}")
        
        # Fetch all countries
        print("Fetching countries...")
        countries = self.fetch_countries()
        print(f"Found {len(countries)} countries")
        
        # Filter out excluded countries
        countries = [c for c in countries if c['code'] not in self.excluded_countries]
        print(f"Processing {len(countries)} countries after exclusions")
        
        # Insert countries
        self.insert_countries(countries)
        
        # Get country IDs from database
        conn = self.connect_db()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT country_id, country_code FROM Countries")
        country_map = {row['country_code']: row['country_id'] for row in cursor.fetchall()}
        cursor.close()
        conn.close()
        
        # Track statistics
        total_records = 0
        
        # Fetch and store data for each country
        for country in countries:
            print(f"Fetching data for {country['name']} ({country['code']})")
            country_id = country_map.get(country['code'])
            
            if not country_id:
                print(f"Country ID not found for {country['name']} ({country['code']}), skipping")
                continue
            
            # Process each selected indicator
            for indicator_key, indicator_code in self.selected_indicators.items():
                table_name = self.get_table_name_for_indicator(indicator_key)
                
                if not table_name:
                    print(f"No table mapping found for indicator {indicator_key}, skipping")
                    continue
                
                print(f"  - Fetching {indicator_key} data...")
                data = self.fetch_indicator_data(
                    country['code'], 
                    indicator_code,
                    start_year=start_year,
                    end_year=end_year
                )
                
                if data:
                    records = self.populate_indicator_table(table_name, country_id, source_id, data)
                    print(f"    Inserted/updated {records} records for {indicator_key}")
                    total_records += records
                else:
                    print(f"    No data available for {indicator_key}")
            
            # Rate limiting to avoid hitting API limits
            time.sleep(1)
            
        print(f"Process complete. Total records inserted/updated: {total_records}")
        return total_records

# Usage example
if __name__ == "__main__":
    config = {
        'user': 'root',       # Your MySQL username
        'password': 'LZ#amhe!32',   # Your MySQL password
        'host': '127.0.0.1',           # The host where MySQL server is running
        'database': 'fyp1',   # The database name where you want to create tables
        'raise_on_warnings': True
    }
    
    fetcher = WorldBankDataFetcher(config)
    
    # Example: Set specific indicators to fetch (comment out to fetch all)
    fetcher.set_selected_indicators(['population'])
    
    # Example: Exclude specific countries (comment out to include all)
    #fetcher.set_excluded_countries(['ZZZ', 'YYY'])  # Replace with actual country codes to exclude
    
    # Run the fetcher with custom year range
    fetcher.fetch_and_store_all_data(start_year=1960, end_year=2023)