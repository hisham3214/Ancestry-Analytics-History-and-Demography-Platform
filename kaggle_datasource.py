import mysql.connector
import pandas as pd

# MySQL connection details
config = {
    'user': 'root',       # Your MySQL username
    'password': 'LZ#amhe!32',   # Your MySQL password
    'host': '127.0.0.1',           # The host where MySQL server is running
    'database': 'fyp1',   # The database name where you want to insert data
    'raise_on_warnings': True
}

# Read Kaggle CSV file
df_kaggle = pd.read_csv("world_population_data.csv")  # Replace with actual Kaggle CSV filename

# Establish MySQL connection
cnx = mysql.connector.connect(**config)
cursor = cnx.cursor()

def insert_data_source(name, website):
    cursor.execute("""
        INSERT INTO Data_Sources (name, website) 
        VALUES (%s, %s) AS new 
        ON DUPLICATE KEY UPDATE website = new.website
    """, (name, website))
    cnx.commit()
    cursor.execute("SELECT source_id FROM Data_Sources WHERE name = %s", (name,))
    result = cursor.fetchone()
    cursor.fetchall()  # Clear any remaining results
    return result[0] if result else None

def get_or_insert_country(country_code, country_name):
    cursor.execute("SELECT country_id FROM Countries WHERE country_code = %s", (country_code,))
    result = cursor.fetchone()
    cursor.fetchall()  # Clear any remaining results
    if result:
        return result[0]
    cursor.execute("""
        INSERT INTO Countries (country_name, country_code)
        VALUES (%s, %s)
    """, (country_name, country_code))
    cnx.commit()
    cursor.execute("SELECT country_id FROM Countries WHERE country_code = %s", (country_code,))
    result = cursor.fetchone()
    cursor.fetchall()  # Clear any remaining results
    return result[0] if result else None

def insert_population_data(country_id, source_id, year, population):
    cursor.execute("""
        INSERT INTO Population (country_id, source_id, year, population)
        VALUES (%s, %s, %s, %s) AS new 
        ON DUPLICATE KEY UPDATE population = new.population
    """, (country_id, source_id, year, population))
    cnx.commit()

kaggle_source_id = insert_data_source("World data population kaggle", "https://www.kaggle.com/datasets/tanishqdublish/world-data-population/data")

# Insert Kaggle data
population_columns = [col for col in df_kaggle.columns if 'population' in col]

for _, row in df_kaggle.iterrows():
    country_id = get_or_insert_country(row['cca3'], row['country'])
    if country_id:
        for pop_col in population_columns:
            year = int(pop_col.split()[0])  # Extract year from column name
            try:
                population = int(row[pop_col])
                insert_population_data(country_id, kaggle_source_id, year, population)
            except ValueError:
                print(f"Skipping invalid population value for {row['country']} in {year}")

print("Kaggle data inserted successfully!")
cursor.close()
cnx.close()
