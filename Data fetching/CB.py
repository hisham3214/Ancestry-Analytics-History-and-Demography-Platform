import mysql.connector

class FetchData:
    def __init__(self, db_config):
        self.db_config = db_config

    def connect_db(self):
        """
        Establish a connection to the MySQL database.
        """
        conn = mysql.connector.connect(**self.db_config)
        return conn

    def map_country_code(self, genc):
        """
        Map GENC code to country code (ISO3).
        This should return the correct country code based on the provided GENC.
        """
        # Implement your country code mapping logic here.
        country_map = {
            "AD": "AND",
            "AE": "ARE",
            # Add all other mappings...
        }
        return country_map.get(genc)

    def populate_census_data(self, headers, records, source_id, genc):
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

            # Ensure the result set is processed before executing another query
            cursor.fetchall()

            result = cursor.fetchone()
            country_name = record[header_indices['NAME']]
            if not result:
                sql_insert_country = "INSERT INTO Countries (country_code, country_name) VALUES (%s, %s)"
                cursor.execute(sql_insert_country, (country_code, country_name))
                conn.commit()  # Commit the insert

            # Get country ID from the database
            sql = "SELECT country_id FROM Countries WHERE country_code = %s"
            cursor.execute(sql, (country_code,))

            # Ensure the result set is processed before executing another query
            cursor.fetchall()

            result = cursor.fetchone()
            if not result:
                print(f"Country not found in database: GENC={genc} (ISO3={country_code})")
                continue
            country_id = result[0]

            # Extract data
            year = record[header_indices['YR']]
            population = record[header_indices['POP']]
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
                sql = "INSERT INTO Death_Rate (country_id, source_id, year, death_rate) VALUES (%s, %s, %s, %s)"
                cursor.execute(sql, (country_id, source_id, year, float(death_rate)))

        # Commit all the changes at once
        conn.commit()
        cursor.close()
        conn.close()

    def fetch_and_store_all_data(self):
        """
        Main function to fetch data and store it into the database.
        """
        # Example of fetching data from the API or file (adjust this as needed)
        headers = ['NAME', 'GENC', 'POP', 'CBR', 'CDR', 'YR']
        records = [
            # Example data (you would replace this with actual data fetching logic)
            ["Albania", "AL", "3000000", "14.5", "8.9", "1950"],
            ["Algeria", "DZ", "8000000", "18.1", "10.0", "1950"],
            # Add other records as needed...
        ]
        source_id = 1  # Adjust as needed

        # Loop over records and populate the database
        for record in records:
            genc = record[1]  # Assuming 'GENC' is in the second column
            self.populate_census_data(headers, records, source_id, genc)

if __name__ == "__main__":
    # Database configuration (update with your actual config)
    db_config = {
        'user': 'root',
        'password': 'khalil_13579',
        'host': '127.0.0.1',
        'database': 'fyp',
    }

    # Create an instance of the FetchData class and call the fetch_and_store_all_data method
    fetcher = FetchData(db_config)
    fetcher.fetch_and_store_all_data()
