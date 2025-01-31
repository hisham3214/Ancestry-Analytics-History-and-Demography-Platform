import mysql.connector
import pandas as pd

# MySQL connection details
config = {
    'user': 'root',       # Your MySQL username
    'password': 'khalil_13579',   # Your MySQL password
    'host': '127.0.0.1',           # The host where MySQL server is running
    'database': 'fyp',   # The database name where you want to insert data
    'raise_on_warnings': True
}

# Read CSV file
df = pd.read_csv("OECD.ELS.SAE,DSD_POPULATION@DF_POP_HIST,1.0+all.csv")  # Replace with actual CSV filename

# Filter data where MEASURE = 'POP'
df_pop = df[df['MEASURE'] == 'POP']

# Aggregate population by country and year
df_pop_grouped = df_pop.groupby(['REF_AREA', 'Reference area', 'TIME_PERIOD'])['OBS_VALUE'].sum().reset_index()

# Establish MySQL connection
cnx = mysql.connector.connect(**config)
cursor = cnx.cursor()

try:
    # Ensure OECD data source exists or insert it
    cursor.execute("""
        INSERT INTO Data_Sources (name, website) 
        VALUES ('OECD', 'https://www.oecd.org/en.html') AS new 
        ON DUPLICATE KEY UPDATE website = new.website
    """)
    cnx.commit()
    cursor.fetchall()  # Clear unread results

    # Retrieve OECD source_id
    cursor.execute("SELECT source_id FROM Data_Sources WHERE name = 'OECD'")
    source_id = cursor.fetchone()[0]
    cursor.fetchall()  # Clear unread results

    for _, row in df_pop_grouped.iterrows():
        country_code = row['REF_AREA']
        country_name = row['Reference area']
        year = row['TIME_PERIOD']
        population = row['OBS_VALUE']

        # Check if the country already exists
        cursor.execute("SELECT country_id FROM Countries WHERE country_code = %s", (country_code,))
        country = cursor.fetchone()
        if country is None:
            # Ensure country exists or insert it
            cursor.execute("""
                INSERT INTO Countries (country_name, country_code)
                VALUES (%s, %s) AS new 
                ON DUPLICATE KEY UPDATE country_name = new.country_name
            """, (country_name, country_code))
            cnx.commit()
            cursor.fetchall()  # Clear unread results
            
            # Retrieve country_id
            cursor.execute("SELECT country_id FROM Countries WHERE country_code = %s", (country_code,))
            country_id = cursor.fetchone()[0]
            cursor.fetchall()  # Clear unread results
        else:
            country_id = country[0]
        
        # Insert population data
        cursor.execute("""
            INSERT INTO Population (country_id, source_id, year, population)
            VALUES (%s, %s, %s, %s) AS new 
            ON DUPLICATE KEY UPDATE population = new.population
        """, (country_id, source_id, year, population))
        cnx.commit()
        cursor.fetchall()  # Clear unread results

    print("Data inserted successfully!")
except mysql.connector.Error as err:
    print(f"Error: {err}")
finally:
    cursor.close()
    cnx.close()
