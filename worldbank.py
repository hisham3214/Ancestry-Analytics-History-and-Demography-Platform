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
        self.indicators = {
            'birth_rate': 'SP.DYN.CBRT.IN',  # Birth rate, crude (per 1,000 people)
            'death_rate': 'SP.DYN.CDRT.IN',  # Death rate, crude (per 1,000 people)
            'population': 'SP.POP.TOTL'       # Population, total
        }
        
    def connect_db(self):
        """Establish database connection"""
        return mysql.connector.connect(**self.db_config)

    def fetch_countries(self):
        """Fetch list of countries from World Bank API"""
        url = f"{self.base_url}/country?format=json&per_page=300"
        response = requests.get(url)
        data = response.json()[1]  # Skip pagination info
        
        countries = []
        for country in data:
            if country['region']['id'] != "NA":  # Skip aggregates/regions
                countries.append({
                    'name': country['name'],
                    'code': country['id']
                })
        return countries

    def insert_countries(self, countries):
        """Insert countries into the database"""
        conn = self.connect_db()
        cursor = conn.cursor()
        
        for country in countries:
            sql = "INSERT INTO Countries (country_name, country_code) VALUES (%s, %s)"
            cursor.execute(sql, (country['name'], country['code']))
        
        conn.commit()
        cursor.close()
        conn.close()

    def insert_data_source(self):
        """Insert World Bank as data source"""
        conn = self.connect_db()
        cursor = conn.cursor()
        
        sql = "INSERT INTO Data_Sources (name, website) VALUES (%s, %s)"
        cursor.execute(sql, ('World Bank', 'https://data.worldbank.org'))
        source_id = cursor.lastrowid
        
        conn.commit()
        cursor.close()
        conn.close()
        return source_id

    def fetch_indicator_data(self, country_code, indicator, start_year=1900, end_year=2025):
        """Fetch specific indicator data for a country"""
        url = f"{self.base_url}/country/{country_code}/indicator/{indicator}"
        params = {
            'format': 'json',
            'date': f"{start_year}:{end_year}",
            'per_page': 100
        }
        
        response = requests.get(url, params=params)
        try:
            data = response.json()[1]  # Skip pagination info
            return data if data else []
        except:
            return []

    def populate_indicator_table(self, table_name, country_id, source_id, indicator_data):
        """Insert indicator data into specified table"""
        conn = self.connect_db()
        cursor = conn.cursor()
        
        value_column = 'population' if table_name == 'Population' else 'birth_rate' if table_name == 'Birth_Rate' else 'death_rate'
        
        for entry in indicator_data:
            if entry['value'] is not None:
                sql = f"INSERT INTO {table_name} (country_id, source_id, year, {value_column}) VALUES (%s, %s, %s, %s)"
                cursor.execute(sql, (country_id, source_id, entry['date'], float(entry['value'])))
        
        conn.commit()
        cursor.close()
        conn.close()

    def fetch_and_store_all_data(self):
        """Main function to orchestrate the entire data fetching and storing process"""
        # Insert World Bank as data source
        source_id = self.insert_data_source()
        
        # Fetch and insert countries
        countries = self.fetch_countries()
        self.insert_countries(countries)
        
        # Get country IDs from database
        conn = self.connect_db()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT country_id, country_code FROM Countries")
        country_map = {row['country_code']: row['country_id'] for row in cursor.fetchall()}
        cursor.close()
        conn.close()
        
        # Fetch and store data for each country
        for country in countries:
            print(f"Fetching data for {country['name']}")
            country_id = country_map[country['code']]
            
            # Birth Rate
            data = self.fetch_indicator_data(country['code'], self.indicators['birth_rate'])
            self.populate_indicator_table('Birth_Rate', country_id, source_id, data)
            
            # Death Rate
            data = self.fetch_indicator_data(country['code'], self.indicators['death_rate'])
            self.populate_indicator_table('Death_Rate', country_id, source_id, data)
            
            # Population
            data = self.fetch_indicator_data(country['code'], self.indicators['population'])
            self.populate_indicator_table('Population', country_id, source_id, data)
            
            # Rate limiting to avoid hitting API limits
            time.sleep(1)

# Usage example
if __name__ == "__main__":
    config = {
        'user': 'root',       # Your MySQL username
        'password': 'MySQLraghid',   # Your MySQL password
        'host': '127.0.0.1',           # The host where MySQL server is running
        'database': 'fyp',   # The database name where you want to create tables
        'raise_on_warnings': True
    }
    
    fetcher = WorldBankDataFetcher(config)
    fetcher.fetch_and_store_all_data()