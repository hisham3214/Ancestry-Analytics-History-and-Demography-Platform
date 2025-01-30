import csv
import mysql.connector
from mysql.connector import errorcode
import sys
from pathlib import Path

# Add the parent directory to the Python path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))

# Import the config dictionary from main.py
from main import config

def insert_data():
    try:
        # Establish a connection to the MySQL database
        cnx = mysql.connector.connect(**config)
        cursor = cnx.cursor()

        # Insert into Data_Sources table
        insert_source_query = """
        INSERT INTO data_sources (name, website)
        VALUES (%s, %s) AS new_source
        ON DUPLICATE KEY UPDATE
            name = new_source.name,
            website = new_source.website;
        """

        data_source = ('World data population kaggle', 'https://www.kaggle.com/datasets/tanishqdublish/world-data-population/data')
        try:
            cursor.execute(insert_source_query, data_source)
            cnx.commit()
        except mysql.connector.Error as err:
            print(f"Error: {err}")


        # Get the source_id of the inserted source
        cursor.execute("SELECT source_id FROM data_sources WHERE name = %s", (data_source[0],))
        source_id = cursor.fetchone()[0]

        # Read the CSV file
        with open('Khalil/world_population_data.csv', mode='r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)

            for row in reader:
                country_name = row['country']
                country_code = row['cca3']

                # Insert into Countries table if not exists
                insert_country_query = """
                INSERT IGNORE INTO countries (country_name, country_code)
                VALUES (%s, %s);
                """
                cursor.execute(insert_country_query, (country_name, country_code))
                
                # Get the country_id of the inserted or existing country
                cursor.execute("SELECT country_id FROM countries WHERE country_code = %s", (country_code,))
                country_id = cursor.fetchone()[0]

                # Insert into Population table
                population_years = ['2023 population', '2022 population', '2020 population', '2015 population', '2010 population', '2000 population', '1990 population', '1980 population', '1970 population']

                for year_column in population_years:
                    year = int(year_column.split()[0])
                    population = row[year_column].replace(',', '').strip()

                    if population:
                        insert_population_query = """
                        INSERT INTO population (country_id, source_id, year, population)
                        VALUES (%s, %s, %s, %s);
                        """
                        cursor.execute(insert_population_query, (country_id, source_id, year, int(population)))

            # Commit the transactions
            cnx.commit()
            print("Data inserted successfully!")

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password.")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist.")
        else:
            print(err)
    finally:
        # Close the cursor and connection
        if 'cursor' in locals():
            cursor.close()
        if 'cnx' in locals():
            cnx.close()

if __name__ == "__main__":
    insert_data()
