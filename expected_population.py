import mysql.connector
from mysql.connector import errorcode

# Replace with your MySQL connection details
config = {
    'user': 'root',       # Your MySQL username
    'password': 'new_password',   # Your MySQL password
    'host': '127.0.0.1',           # The host where MySQL server is running
    'database': 'fyp',   # The database name where your tables live
    'raise_on_warnings': True
}

# Establish a connection to the MySQL database
cnx = mysql.connector.connect(**config)
cursor = cnx.cursor()

try:
    # 1. Alter the Population table to add new columns
    alter_sql = """
    ALTER TABLE Population
      ADD COLUMN expected_population BIGINT NULL,
      ADD COLUMN difference_pct FLOAT NULL;
    """
    cursor.execute(alter_sql)
    cnx.commit()
    print("Added columns: expected_population, difference_pct to Population table.")

    # 2. Fetch all population records
    cursor.execute("SELECT country_id, year, population FROM Population;")
    records = cursor.fetchall()

    for country_id, year, actual_pop in records:
        # Skip if year is the first entry (no previous year)
        prev_year = year - 1
        cursor.execute(
            "SELECT population FROM Population WHERE country_id=%s AND year=%s",
            (country_id, prev_year)
        )
        prev = cursor.fetchone()
        if not prev:
            continue
        prev_pop = prev[0]

        # Get births for this year
        cursor.execute(
            "SELECT birth_rate FROM Birth_Rate WHERE country_id=%s AND year=%s",
            (country_id, year)
        )
        br = cursor.fetchone()
        births = br[0] if br else 0

        # Get deaths for this year
        cursor.execute(
            "SELECT death_rate FROM Death_Rate WHERE country_id=%s AND year=%s",
            (country_id, year)
        )
        dr = cursor.fetchone()
        deaths = dr[0] if dr else 0

        # Get net migration for this year
        cursor.execute(
            "SELECT net_migration FROM Total_Net_Migration WHERE country_id=%s AND year=%s",
            (country_id, year)
        )
        nm = cursor.fetchone()
        migration = nm[0] if nm else 0

        # Calculate expected population
        expected = prev_pop + births - deaths + migration

        # Calculate difference percentage: (actual - expected) / expected * 100
        diff_pct = None
        if expected:
            diff_pct = (actual_pop - expected) / expected * 100

        # Update the Population table
        cursor.execute(
            """
            UPDATE Population
            SET expected_population = %s,
                difference_pct = %s
            WHERE country_id = %s AND year = %s
            """,
            (expected, diff_pct, country_id, year)
        )

    # Commit all updates
    cnx.commit()
    print("Population table updated with expected_population and difference_pct.")

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_BAD_FIELD_ERROR:
        print("One of the columns might already exist or the table structure is different.")
    else:
        print(f"Error: {err}")

finally:
    cursor.close()
    cnx.close()
