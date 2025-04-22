import mysql.connector
import pandas as pd

# MySQL connection details
config = {
    'user': 'root',
    'password': 'khalil_13579',
    'host': '127.0.0.1',
    'database': 'fyp',
    'raise_on_warnings': True
}

# Read CSV file
df = pd.read_csv("OECD.ELS.SAE,DSD_POPULATION@DF_POP_HIST,1.0+all.csv")  # Replace with actual CSV filename

# Filter data where MEASURE = 'POP'
df_pop = df[df['MEASURE'] == 'POP']

# Group by 'REF_AREA', 'Reference area', and 'TIME_PERIOD' and sum 'OBS_VALUE'
# This groups by country, year, sex, and age so that you don’t merge across distinct sub-populations.
df_pop_grouped = df_pop.groupby(['REF_AREA', 'Reference area', 'TIME_PERIOD', 'SEX', 'AGE'])['OBS_VALUE'].sum().reset_index()
# Establish MySQL connection
cnx = mysql.connector.connect(**config)
cursor = cnx.cursor()

try:
    # Ensure OECD data source exists or insert it
    cursor.execute("""
        INSERT INTO Data_Sources (name, website) 
        VALUES ('OECD', 'https://www.oecd.org/en.html')
        ON DUPLICATE KEY UPDATE website = 'https://www.oecd.org/en.html'
    """)
    cnx.commit()
    cursor.fetchall()  # Clear unread results

    # Retrieve OECD source_id
    cursor.execute("SELECT source_id FROM Data_Sources WHERE name = 'OECD'")
    source_id = cursor.fetchone()[0]
    cursor.fetchall()  # Clear unread results
    print("YES")
    for _, row in df_pop_grouped.iterrows():
        print("OUTER")
        country_code = row['REF_AREA']
        country_name = row['Reference area']
        year = row['TIME_PERIOD']
        population = row['OBS_VALUE']

        # Check if the country already exists
        cursor.execute("SELECT country_id FROM Countries WHERE country_code = %s", (country_code,))
        country = cursor.fetchone()
        cursor.fetchall()  # Clear unread results

        if country is None:
            print("Country not found")
            print(f"Inserrting country {country_name}")
            # Insert new country if it doesn't exist
            cursor.execute("""
                INSERT INTO Countries (country_name, country_code)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE country_name = %s
            """, (country_name, country_code, country_name))
            cnx.commit()
            cursor.fetchall()  # Clear unread results
            
            # Retrieve country_id
            cursor.execute("SELECT country_id FROM Countries WHERE country_code = %s", (country_code,))
            country_id = cursor.fetchone()[0]
            cursor.fetchall()  # Clear unread results
            print(f"country inserted successfuly")
        else:
            print(f"country found {country_name}")
            country_id = country[0]
        
        print("inserting pop data")
        # Insert population data
        cursor.execute("""
            INSERT INTO Population (country_id, source_id, year, population)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE population = %s
        """, (country_id, source_id, year, population, population))
        cnx.commit()
        cursor.fetchall()  # Clear unread results
        print(f"pop data inserted successfuly")

    print("Data inserted successfully!")
except mysql.connector.Error as err:
    print(f"Error: {err}")
finally:
    cursor.close()
    cnx.close()
print("Done")