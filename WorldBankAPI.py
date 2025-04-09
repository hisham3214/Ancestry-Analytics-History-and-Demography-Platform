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
            
            # Add under-five mortality indicators
            'under_five_mortality_male': 'SH.DYN.MORT.MA',  # Under-5 mortality rate, male
            'under_five_mortality_female': 'SH.DYN.MORT.FE', # Under-5 mortality rate, female
            
            # Other existing indicators...
            'sex_ratio_at_birth': 'SP.POP.BRTH.MF',
            'infant_mortality_male': 'SP.DYN.IMRT.MA.IN',
            'infant_mortality_female': 'SP.DYN.IMRT.FE.IN',
            'life_expectancy_male': 'SP.DYN.LE00.MA.IN',
            'life_expectancy_female': 'SP.DYN.LE00.FE.IN',
            'population_0_14': 'SP.POP.0014.TO.ZS',
            'population_15_64': 'SP.POP.1564.TO.ZS',
            'population_65plus': 'SP.POP.65UP.TO.ZS',
            'population_male': 'SP.POP.TOTL.MA.IN',
            'population_female': 'SP.POP.TOTL.FE.IN',
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
        per_page = 32767  # Increase this to get more data per request
        more_pages = True
        total_pages = None
        
        print(f"    Fetching data for {country_code}/{indicator}, starting page {page}")
        
        max_retries = 3
        
        while more_pages:
            retries = 0
            success = False
            
            while retries < max_retries and not success:
                try:
                    url = f"{self.base_url}/country/{country_code}/indicator/{indicator}"
                    params = {
                        'format': 'json',
                        'date': f"{start_year}:{end_year}",
                        'page': page,
                        'per_page': per_page
                    }
                    response = requests.get(url, params=params, timeout=30)
                    
                    if response.status_code == 200:
                        success = True
                    else:
                        print(f"    Attempt {retries+1}: API returned status {response.status_code}")
                        retries += 1
                        time.sleep(2 * retries)  # Exponential backoff
                except Exception as e:
                    print(f"    Attempt {retries+1}: Error: {str(e)[:100]}")
                    retries += 1
                    time.sleep(2 * retries)  # Exponential backoff
            
            if not success:
                print(f"    Failed to fetch data after {max_retries} attempts")
                break
            
            try:
                json_data = response.json()
                
                # Check if we have valid data with pagination
                if isinstance(json_data, list) and len(json_data) > 1:
                    pagination = json_data[0]
                    data = json_data[1]
                    
                    # Store total pages on first response
                    if total_pages is None:
                        total_pages = pagination.get('pages', 1)
                        print(f"    API reports {pagination.get('total', 0)} total records across {total_pages} pages")
                    
                    if data:
                        all_data.extend(data)
                        print(f"    Page {page}/{total_pages}: Got {len(data)} records (total so far: {len(all_data)})")
                    else:
                        print(f"    Page {page}/{total_pages}: No data returned")
                    
                    # Check if we need to fetch more pages
                    if pagination.get('page', 1) < pagination.get('pages', 1):
                        page += 1
                    else:
                        more_pages = False
                        print(f"    Reached last page. Total records: {len(all_data)}")
                else:
                    more_pages = False
                    if isinstance(json_data, list) and len(json_data) == 1:
                        print(f"    API returned message: {json_data[0].get('message', 'No message')}")
                    else:
                        print(f"    Unexpected response format: {json_data}")
            except Exception as e:
                print(f"    Error fetching data: {str(e)[:100]}")
                more_pages = False
            
            # Rate limiting
            time.sleep(0.5)
        
        print(f"    Total records fetched: {len(all_data)}")
        return all_data

    def fetch_indicator_data_with_chunks(self, country_code, indicator, start_year=1900, end_year=2025):
        """Fetch data in year chunks to work around API limitations"""
        all_data = []
        chunk_size = 20  # 20 years at a time
        
        for chunk_start in range(start_year, end_year + 1, chunk_size):
            chunk_end = min(chunk_start + chunk_size - 1, end_year)
            print(f"    Fetching year chunk {chunk_start}-{chunk_end}")
            
            chunk_data = self.fetch_indicator_data(
                country_code, indicator, start_year=chunk_start, end_year=chunk_end
            )
            all_data.extend(chunk_data)
            
            # Avoid hitting rate limits
            time.sleep(1)
        
        return all_data

    def get_table_name_for_indicator(self, indicator_key):
        """Convert indicator key to corresponding table name"""
        table_mapping = {
            'birth_rate': 'Birth_Rate',
            'death_rate': 'Death_Rate',
            'population': 'Population',
            'net_migration': 'Total_Net_Migration',
            'fertility_rate': 'Fertility_Rate',
            
            # Add under-five mortality mappings
            'under_five_mortality_male': 'Under_Five_Mortality_Rate_By_Sex',
            'under_five_mortality_female': 'Under_Five_Mortality_Rate_By_Sex',
            
            # Existing mappings...
            'sex_ratio_at_birth': 'Sex_Ratio_At_Birth',
            'infant_mortality_male': 'Infant_Mortality_Rate_By_Sex',
            'infant_mortality_female': 'Infant_Mortality_Rate_By_Sex',
            'life_expectancy_male': 'life_expectancy_at_birth_by_sex',
            'life_expectancy_female': 'life_expectancy_at_birth_by_sex',
            'population_0_14': 'Population_By_Age_Group',
            'population_15_64': 'Population_By_Age_Group',
            'population_65plus': 'Population_By_Age_Group',
            'population_male': 'Population_by_sex',
            'population_female': 'Population_by_sex',
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
            
            # Add under-five mortality mapping
            'Under_Five_Mortality_Rate_By_Sex': 'mortality_rate',
            
            # Existing mappings...
            'Infant_Mortality_Rate_By_Sex': 'infant_mortality_rate',
            'life_expectancy_at_birth_by_sex': 'life_expectancy',
            'Population_By_Age_Group': 'population',
            'Population_by_sex': 'population',
            'Sex_Ratio_At_Birth': 'sex_ratio_at_birth',
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
        
        # new:
        # Special handling for sex-specific indicators and age groups
        is_sex_specific = (table_name == 'Infant_Mortality_Rate_By_Sex' or 
                          table_name == 'life_expectancy_at_birth_by_sex' or
                          table_name == 'Population_by_sex' or
                          table_name == 'Under_Five_Mortality_Rate_By_Sex') and hasattr(self, 'current_indicator_key')
        is_age_group = table_name == 'Population_By_Age_Group' and hasattr(self, 'current_indicator_key')
        
        sex = None
        age_group_id = None
        age_group_label = None
        age_start = None
        age_end = None
        
        # And update the sex determination logic
        if is_sex_specific:
            if (self.current_indicator_key == 'infant_mortality_male' or 
                self.current_indicator_key == 'life_expectancy_male' or
                self.current_indicator_key == 'population_male' or
                self.current_indicator_key == 'under_five_mortality_male'):
                sex = 'Male'
            elif (self.current_indicator_key == 'infant_mortality_female' or 
                  self.current_indicator_key == 'life_expectancy_female' or
                  self.current_indicator_key == 'population_female' or
                  self.current_indicator_key == 'under_five_mortality_female'):
                sex = 'Female'
        
        if is_age_group:
            if self.current_indicator_key == 'population_0_14':
                age_group_id = 1
                age_group_label = '0-14'
                age_start = 0
                age_end = 14
            elif self.current_indicator_key == 'population_15_64':
                age_group_id = 2
                age_group_label = '15-64'
                age_start = 15
                age_end = 64
            elif self.current_indicator_key == 'population_65plus':
                age_group_id = 3
                age_group_label = '65+'
                age_start = 65
                age_end = 999  # Using 999 to represent "and above"
        
        # Prepare all records to insert in batch
        batch_data = []
        for entry in indicator_data:
            if entry['value'] is not None:
                try:
                    # Base data for all tables
                    record_data = [
                        country_id, 
                        source_id, 
                        entry['date'], 
                        float(entry['value']),
                        datetime.now()
                    ]
                    
                    # Add additional fields based on table type
                    if is_sex_specific and sex:
                        record_data.insert(3, sex)  # Insert sex before the value
                    elif is_age_group:
                        # Insert all age group fields before the value
                        record_data.insert(3, sex)  # Sex is NULL for age group data
                        record_data.insert(4, age_group_id)
                        record_data.insert(5, age_group_label)
                        record_data.insert(6, age_start)
                        record_data.insert(7, age_end)
                    
                    batch_data.append(tuple(record_data))
                except Exception as e:
                    print(f"Error preparing data: {e}")
        
        # Process in smaller batches
        batch_size = 50
        for i in range(0, len(batch_data), batch_size):
            batch_chunk = batch_data[i:i+batch_size]
            try:
                # SQL query depends on whether we're handling sex-specific data or age groups
                if is_sex_specific:
                    sql = f"""
                    INSERT INTO {table_name} 
                    (country_id, source_id, year, sex, {value_column}, last_updated)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    AS new_values
                    ON DUPLICATE KEY UPDATE
                    {value_column} = new_values.{value_column},
                    last_updated = new_values.last_updated
                    """
                elif is_age_group:
                    sql = f"""
                    INSERT INTO {table_name} 
                    (country_id, source_id, year, sex, age_group_id, 
                    age_group_label, age_start, age_end, {value_column}, last_updated)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    AS new_values
                    ON DUPLICATE KEY UPDATE
                    {value_column} = new_values.{value_column},
                    last_updated = new_values.last_updated
                    """
                else:
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
                conn.commit()
                records_inserted += len(batch_chunk)
            except Exception as e:
                print(f"Error inserting batch data: {e}")
        
        cursor.close()
        conn.close()
        
        return records_inserted

    def calculate_and_store_sex_ratio_total(self, country_id, source_id, start_year=1900, end_year=2025):
        """Calculate and store sex ratio of total population (males per female)"""
        print(f"  - Calculating sex ratio for total population...")
        
        # Get data from database - this is more efficient than re-fetching from API
        conn = self.connect_db()
        cursor = conn.cursor(buffered=True, dictionary=True)
        
        # Get male population data
        cursor.execute("""
            SELECT year, population 
            FROM Population_by_sex 
            WHERE country_id = %s AND source_id = %s AND sex = 'Male'
            AND year BETWEEN %s AND %s
        """, (country_id, source_id, start_year, end_year))
        male_data = {row['year']: row['population'] for row in cursor.fetchall()}
        
        # Get female population data
        cursor.execute("""
            SELECT year, population 
            FROM Population_by_sex 
            WHERE country_id = %s AND source_id = %s AND sex = 'Female'
            AND year BETWEEN %s AND %s
        """, (country_id, source_id, start_year, end_year))
        female_data = {row['year']: row['population'] for row in cursor.fetchall()}
        
        # Calculate sex ratio for all years where we have both male and female data
        records_inserted = 0
        batch_data = []
        
        for year in set(male_data.keys()) & set(female_data.keys()):
            male_pop = male_data[year]
            female_pop = female_data[year]
            
            # Check for division by zero
            if female_pop and female_pop > 0:
                # Males per female ratio
                sex_ratio = male_pop / female_pop
                batch_data.append((country_id, source_id, year, sex_ratio, datetime.now()))
        
        # Insert data in batches
        if batch_data:
            batch_size = 50
            for i in range(0, len(batch_data), batch_size):
                batch_chunk = batch_data[i:i+batch_size]
                try:
                    sql = """
                    INSERT INTO Sex_Ratio_Total_Population
                    (country_id, source_id, year, sex_ratio, last_updated)
                    VALUES (%s, %s, %s, %s, %s)
                    AS new_values
                    ON DUPLICATE KEY UPDATE
                    sex_ratio = new_values.sex_ratio,
                    last_updated = new_values.last_updated
                    """
                    cursor.executemany(sql, batch_chunk)
                    conn.commit()
                    records_inserted += len(batch_chunk)
                except Exception as e:
                    print(f"Error inserting sex ratio data: {e}")
        
        cursor.close()
        conn.close()
        
        print(f"    Inserted/updated {records_inserted} records for sex ratio total")
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
                # Store the current indicator key for sex-specific processing
                self.current_indicator_key = indicator_key
                
                table_name = self.get_table_name_for_indicator(indicator_key)
                
                if not table_name:
                    print(f"No table mapping found for indicator {indicator_key}, skipping")
                    continue
                
                print(f"  - Fetching {indicator_key} data...")
                data = self.fetch_indicator_data_with_chunks(
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
            
            # After processing all indicators for this country, calculate derived indicators
            # Only calculate if we have both male and female population indicators selected
            if 'population_male' in self.selected_indicators and 'population_female' in self.selected_indicators:
                # Calculate sex ratio for total population
                records = self.calculate_and_store_sex_ratio_total(country_id, source_id, start_year, end_year)
                total_records += records
            
            # Rate limiting to avoid hitting API limits
            time.sleep(1)
            
        print(f"Process complete. Total records inserted/updated: {total_records}")
        return total_records

# Usage example
if __name__ == "__main__":
    config = {
        'user': 'root',       # Your MySQL username
        'password': 'new_password',   # Your MySQL password
        'host': '127.0.0.1',           # The host where MySQL server is running
        'database': 'fyp',   # The database name where you want to create tables
        'raise_on_warnings': True
    }
    
    fetcher = WorldBankDataFetcher(config)
    
    # Example: Set specific indicators to fetch (comment out to fetch all)
    # fetcher.set_selected_indicators(['population'])
    
    # Example: Exclude specific countries (comment out to include all)
    #fetcher.set_excluded_countries(['ZZZ', 'YYY'])  # Replace with actual country codes to exclude
    
    # Run the fetcher with custom year range
    fetcher.fetch_and_store_all_data(start_year=1960, end_year=2023)