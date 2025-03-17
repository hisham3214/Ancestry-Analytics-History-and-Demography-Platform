import requests
import mysql.connector
import urllib.parse
import time
import re
from datetime import datetime
class CensusBureauDataFetcher:
    
    def __init__(self, db_config):
        """
        Initialize the data fetcher with database configuration
        db_config should be a dictionary with: host, user, password, database
        """

        url_countries = f"https://api.census.gov/data/timeseries/idb/1year?get=NAME,GENC&YR=2023&AGE=20&SEX=0&key=c4805d0810f8673f0daf941bd63f239958b66fd4"
        response = requests.get(url_countries)
        countries = response.json()

        # Extract country codes (excluding the first row which contains column names)
        genc_codes = [row[1] for row in countries[1:]]
        self.genc_codes = genc_codes

        print("Total countries found:", len(genc_codes))
        

        self.db_config = db_config
        self.base_url = "https://api.census.gov/data/timeseries/idb/5year"
        self.api_key = "c4805d0810f8673f0daf941bd63f239958b66fd4"  # Your API key
        self.FIPS_TO_ISO3 = {
            "AF": "AFG", "AL": "ALB", "DZ": "DZA", "AD": "AND", "AO": "AGO", "AR": "ARG", "AM": "ARM",
            "AU": "AUS", "AT": "AUT", "AZ": "AZE", "BS": "BHS", "BH": "BHR", "BD": "BGD", "BB": "BRB",
            "BY": "BLR", "BE": "BEL", "BZ": "BLZ", "BJ": "BEN", "BT": "BTN", "BO": "BOL", "BA": "BIH",
            "BW": "BWA", "BR": "BRA", "BN": "BRN", "BG": "BGR", "BF": "BFA", "BI": "BDI", "KH": "KHM",
            "CM": "CMR", "CA": "CAN", "CV": "CPV", "CF": "CAF", "TD": "TCD", "CL": "CHL", "CN": "CHN",
            "CO": "COL", "KM": "COM", "CG": "COG", "CD": "COD", "CR": "CRI", "CI": "CIV", "HR": "HRV",
            "CU": "CUB", "CY": "CYP", "CZ": "CZE", "DK": "DNK", "DJ": "DJI", "DO": "DOM", "EC": "ECU",
            "EG": "EGY", "SV": "SLV", "GQ": "GNQ", "ER": "ERI", "EE": "EST", "ET": "ETH", "FJ": "FJI",
            "FI": "FIN", "FR": "FRA", "GA": "GAB", "GM": "GMB", "GE": "GEO", "DE": "DEU", "GH": "GHA",
            "GR": "GRC", "GT": "GTM", "GN": "GIN", "GW": "GNB", "GY": "GUY", "HT": "HTI", "HN": "HND",
            "HU": "HUN", "IS": "ISL", "IN": "IND", "ID": "IDN", "IR": "IRN", "IQ": "IRQ", "IE": "IRL",
            "IL": "ISR", "IT": "ITA", "JM": "JAM", "JP": "JPN", "JO": "JOR", "KZ": "KAZ", "KE": "KEN",
            "KI": "KIR", "KR": "KOR", "KW": "KWT", "KG": "KGZ", "LA": "LAO", "LV": "LVA", "LB": "LBN",
            "LS": "LSO", "LR": "LBR", "LY": "LBY", "LT": "LTU", "LU": "LUX", "MG": "MDG", "MW": "MWI",
            "MY": "MYS", "MV": "MDV", "ML": "MLI", "MT": "MLT", "MR": "MRT", "MU": "MUS", "MX": "MEX",
            "MD": "MDA", "MN": "MNG", "ME": "MNE", "MA": "MAR", "MZ": "MOZ", "MM": "MMR", "NA": "NAM",
            "NP": "NPL", "NL": "NLD", "NZ": "NZL", "NI": "NIC", "NE": "NER", "NG": "NGA", "NO": "NOR",
            "OM": "OMN", "PK": "PAK", "PA": "PAN", "PG": "PNG", "PY": "PRY", "PE": "PER", "PH": "PHL",
            "PL": "POL", "PT": "PRT", "QA": "QAT", "RO": "ROU", "RU": "RUS", "RW": "RWA", "WS": "WSM",
            "SA": "SAU", "SN": "SEN", "RS": "SRB", "SC": "SYC", "SL": "SLE", "SG": "SGP", "SK": "SVK",
            "SI": "SVN", "SB": "SLB", "SO": "SOM", "ZA": "ZAF", "ES": "ESP", "LK": "LKA", "SD": "SDN",
            "SR": "SUR", "SZ": "SWZ", "SE": "SWE", "CH": "CHE", "SY": "SYR", "TJ": "TJK", "TZ": "TZA",
            "TH": "THA", "TL": "TLS", "TG": "TGO", "TO": "TON", "TT": "TTO", "TN": "TUN", "TR": "TUR",
            "TM": "TKM", "UG": "UGA", "UA": "UKR", "AE": "ARE", "GB": "GBR", "US": "USA", "UY": "URY",
            "UZ": "UZB", "VU": "VUT", "VE": "VEN", "VN": "VNM", "YE": "YEM", "ZM": "ZMB", "ZW": "ZWE",
            "LC":"LCA","LI":"LIE","PM":"SPM", "VC":"VCT", "PS":"PSE","TV":"TUV","AS":"ASM","BM":"BMU",
            "BQ":"BES","BV":"BVT","IO":"IOT","KY":"CYM","CX":"CXR","CC":"CCK","CK":"COK","CW":"CUW",
            "DM":"DMA","FK":"FLK","FO":"FRO","GF":"GUF","PF":"PYF","TF":"ATF","XG":"GAZ","XW":"WES",
            "AW": "ABW","AI":"AIA","AG":"ATG","FM":"FSM","GD":"GRD","GG":"GGY","GI":"GIB","GL":"GRL",
            "GU":"GUM","HK":"HKG","IM":"IMN","JE":"JEY","KN":"KNA","KP":"PRK","MC":"MCO","MF":"MAF",
            "MH":"MHL","MO":"MAC","MP":"MNP","MS":"MSR","NC":"NCL","NR":"NRU","PW":"PLW","SH":"SHN",
            "SM":"SMR","SS":"SSD","SX":"SXM","TC":"TCA","TW":"TWN","VG":"VGB","WF":"WLF","XK":"KOS",
            "BL":"BLM","MK":"MKD","PR":"PRI","ST":"STP","VI":"VIR"
        }

        self.indicators = {
            'population': 'POP',  # Population
            'birth_rate': 'CBR',  # Crude Birth Rate
            'death_rate': 'CDR',   # Crude Death Rate
            'fertility_rate': 'TFR',
            'sex_ratio_population': 'SEXRATIO',
            'sex_ratio_birth': 'SRB'
        }

    def connect_db(self):
        """Establish database connection"""
        return mysql.connector.connect(**self.db_config)


    def fetch_census_data(self,genc):
        """
        Fetch data from the Census Bureau API for a specific year
        """
        params = {
            'get': 'NAME,GENC,group(IDB5YEAR),POP,CBR,CDR,YR',  # Include relevant fields
            'for': f'genc+standard+countries+and+areas:{genc}',  # Query data for all countries
            'YR': '1950:2024',                   # Year of data
            'key': self.api_key                # API key
        }
        print(f"Requesting with params: {params}") 
        encoded_params = urllib.parse.urlencode(params, safe='+,:')  # Preserve `+` and `:` in query
        response = requests.get(self.base_url, params=encoded_params)
        if response.status_code == 200:
            data = response.json()
            headers = data[0]  # First row contains headers
            records = data[1:]  # Remaining rows contain data
            return headers, records
        else:
            print(f"Failed to fetch data for years: {response.status_code}")
            print(f"Response content: {response.text}")
            return None, None

    def map_country_code(self, genc_code):
        """
        Map GENC code from Census Bureau to ISO Alpha-3 country code
        """
        return self.FIPS_TO_ISO3.get(genc_code, None)

    def populate_census_data(self, headers, records, source_id,genc):
        """
        Insert Census Bureau data into the database
        """
        conn = self.connect_db()
        cursor = conn.cursor()

        # Map headers to indices
        header_indices = {header: idx for idx, header in enumerate(headers)}

        for record in records:
            country_code = self.map_country_code(genc)

            if not country_code:
                print(f"Skipping unknown GENC code: {genc}")
                continue

            # Check if country exists, insert if not
            sql_check_country = "SELECT country_id FROM Countries WHERE country_code = %s"
            cursor.execute(sql_check_country, (country_code,))
            result = cursor.fetchone()

            country_name= record[header_indices['NAME']]
            if not result:
                sql_insert_country = "INSERT INTO Countries (country_code, country_name) VALUES (%s, %s)"
                cursor.execute(sql_insert_country, (country_code, country_name))
                conn.commit() 


            # Get country ID from the database
            sql = "SELECT country_id FROM Countries WHERE country_code = %s"
            cursor.execute(sql, (country_code,))
            result = cursor.fetchone()
            if not result:
                print(f"Country not found in database: GENC={genc} (ISO3={country_code})")
                continue
            country_id = result[0]

            # Extract data
            year= record[header_indices['YR']]
            birth_rate = record[header_indices['CBR']]
            fertility_rate= record[header_indices['TFR']]
            total_net_migration=record[header_indices['NIM']]
            crude_net_migration=record[header_indices['NMR']]
            death_rate = record[header_indices['CDR']]
            population = record[header_indices['POP']]
            sex_ratio_birth= record[header_indices['SRB']]
            sex_ratio_population= record[header_indices['SEXRATIO']]
            median_age= record[header_indices['MEDAGE']]
            
            

            # Insert into respective tables
            if population and population != '-':
                sql = "INSERT INTO Population (country_id, source_id, year, population,last_updated) VALUES (%s, %s, %s, %s,%s)"
                cursor.execute(sql, (country_id, source_id, year, int(population),datetime.now()))

            if birth_rate and birth_rate != '-':
                sql = "INSERT INTO Birth_Rate (country_id, source_id, year, birth_rate,last_updated) VALUES (%s, %s, %s, %s,%s)"
                cursor.execute(sql, (country_id, source_id, year, float(birth_rate),datetime.now()))

            if death_rate and death_rate != '-':
                sql = "INSERT INTO Death_Rate (country_id, source_id, year, death_rate,last_updated) VALUES (%s, %s, %s, %s,%s)"
                cursor.execute(sql, (country_id, source_id, year, float(death_rate),datetime.now()))

            if fertility_rate and fertility_rate != '-':
                sql = "INSERT INTO Fertility_Rate (country_id, source_id, year, Fertility_rate,last_updated) VALUES (%s, %s, %s, %s,%s)"
                cursor.execute(sql, (country_id, source_id, year, float(fertility_rate),datetime.now()))

            if sex_ratio_population and sex_ratio_population != '-':
                sql = "INSERT INTO Sex_Ratio_Total_Population (country_id, source_id, year, sex_ratio,last_updated) VALUES (%s, %s, %s, %s,%s)"
                cursor.execute(sql, (country_id, source_id, year, float(sex_ratio_population),datetime.now()))

            if sex_ratio_birth and sex_ratio_birth != '-':
                sql = "INSERT INTO Sex_Ratio_At_Birth (country_id, source_id, year, sex_ratio_at_birth,last_updated) VALUES (%s, %s, %s, %s,%s)"
                cursor.execute(sql, (country_id, source_id, year, float(sex_ratio_birth),datetime.now()))

            if median_age and median_age != '-':
                sql = "INSERT INTO median_age (country_id, source_id, year, age,last_updated) VALUES (%s, %s, %s, %s,%s)"
                cursor.execute(sql, (country_id, source_id, year, float(median_age),datetime.now()))

            if crude_net_migration and crude_net_migration != '-':
                sql = "INSERT INTO crude_net_migration_rate (country_id, source_id, year, migration_rate,last_updated) VALUES (%s, %s, %s, %s,%s)"
                cursor.execute(sql, (country_id, source_id, year, float(crude_net_migration),datetime.now()))

            if total_net_migration and total_net_migration != '-':
                sql = "INSERT INTO total_net_migration (country_id, source_id, year, net_migration,last_updated) VALUES (%s, %s, %s, %s,%s)"
                cursor.execute(sql, (country_id, source_id, year, int(total_net_migration),datetime.now()))

            for header, index in header_indices.items():
                     match = re.match(r'([MF])?POP(\d+)_(\d+)', header)
                     if match:
                        sex_code = match.group(1)  # 'M' or 'F'
                        age_start = int(match.group(2))  # e.g., 5
                        age_end = int(match.group(3))    # e.g., 9
                        
                        if sex_code == 'M':
                            sex = "Man"
                            sex_id = 1
                        elif sex_code == 'F':
                            sex = "Woman"
                            sex_id = 2
                        else:
                            sex = "Both"
                            sex_id = None  # You can decide: NULL or a specific ID (like 3)
                        
                        population_value = record[index]
                        if population_value and population_value != '-':
                            sql = "INSERT INTO Population_By_Age_Group (country_id, source_id, year, sex_id, sex, age_start, age_end, population, last_updated) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                            cursor.execute(sql, (
                                country_id, source_id, year, sex_id, sex, age_start, age_end, 
                                float(population_value), datetime.now()
                            ))

            for header, index in header_indices.items():
                if header.startswith('IMR'):  # Capture all IMR indicators
                    if header == 'IMR':
                        sex = 'Both'
                        sex_id = None  # or 3, based on preference
                    elif header == 'IMR_M':
                        sex = 'Man'
                        sex_id = 1
                    elif header == 'IMR_F':
                        sex = 'Woman'
                        sex_id = 2
                    
                    imr_value = record[index]
                    if imr_value and imr_value != '-':
                        sql = "INSERT INTO Infant_Mortality_Rate_By_Sex (country_id, source_id, year, sex_id, sex, infant_mortality_rate, last_updated) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                        cursor.execute(sql, (
                            country_id, source_id, year, sex_id, sex, float(imr_value), datetime.now()
                        ))
            for header, index in header_indices.items():
                if header.startswith('E0'):  # Capture E0, E0_M, E0_F
                    if header == 'E0':
                        sex = 'Both'
                        sex_id = None  # or 3, optional
                    elif header == 'E0_M':
                        sex = 'Man'
                        sex_id = 1
                    elif header == 'E0_F':
                        sex = 'Woman'
                        sex_id = 2
                    
                    life_expectancy_value = record[index]
                    if life_expectancy_value and life_expectancy_value != '-':
                        sql = "INSERT INTO life_expectancy_at_birth_by_sex (country_id, source_id, year, sex_id, sex, life_expectancy, last_updated) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                        cursor.execute(sql, (
                            country_id, source_id, year, sex_id, sex, float(life_expectancy_value), datetime.now()
                        ))
            for header, index in header_indices.items():
                if header in ['MR0_4', 'MMR0_4', 'FMR0_4']:
                    # Determine sex and sex_id
                    if header == 'MR0_4':
                        sex = 'Both'
                        sex_id = None  # or 3
                    elif header == 'MMR0_4':
                        sex = 'Man'
                        sex_id = 1
                    elif header == 'FMR0_4':
                        sex = 'Woman'
                        sex_id = 2
                    
                    mortality_rate_value = record[index]
                    if mortality_rate_value and mortality_rate_value != '-':
                        sql = "INSERT INTO Under_Five_Mortality_Rate_By_Sex (country_id, source_id, year, sex_id, sex, mortality_rate, last_updated) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                        cursor.execute(sql, (
                            country_id, source_id, year, sex_id, sex, float(mortality_rate_value), datetime.now()
                        ))
            for header, index in header_indices.items():
                if header == 'MPOP' or header == 'FPOP':
                    # Determine sex and sex_id
                    if header == 'MPOP':
                        sex = 'Man'
                        sex_id = 1
                    elif header == 'FPOP':
                        sex = 'Woman'
                        sex_id = 2
                    
                    population_value = record[index]
                    if population_value and population_value != '-':
                        sql = "INSERT INTO Population_by_sex (country_id, source_id, year, sex_id, sex, population, last_updated) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                        cursor.execute(sql, (
                            country_id, source_id, year, sex_id, sex, float(population_value), datetime.now()
                        ))

        conn.commit()
        cursor.close()
        conn.close()

    def fetch_and_store_all_data(self):
        """Main function to orchestrate the entire data fetching and storing process"""
        # Insert Census Bureau as data source
        conn = self.connect_db()
        cursor = conn.cursor()
        sql_check = "SELECT source_id FROM Data_Sources WHERE name = %s"
        cursor.execute(sql_check, ('Census Bureau',))
        existing = cursor.fetchone()
        if not existing:
            # Insert only if not found
            sql_insert = "INSERT INTO Data_Sources (name, website) VALUES (%s, %s)"
            cursor.execute(sql_insert, ('Census Bureau', 'https://www.census.gov'))
            source_id = cursor.lastrowid
            conn.commit()
        else:
            source_id = existing[0]  # Get existing ID

        cursor.close()
        conn.close()

        # Fetch and store data for each year
        for genc in self.genc_codes:
            headers, records = self.fetch_census_data(genc)
            if headers and records:
                self.populate_census_data(headers, records, source_id, genc)


# Usage example
if __name__ == "__main__":
    config = {
        'user': 'root',       # Your MySQL username
        'password': 'MySQLraghid',   # Your MySQL password
        'host': '127.0.0.1',           # The host where MySQL server is running
        'database': 'fyp',   # The database name where you want to create tables
        'raise_on_warnings': True
    }

    fetcher = CensusBureauDataFetcher(config)
    fetcher.fetch_and_store_all_data()