import mysql.connector
from mysql.connector import errorcode

# Replace with your MySQL connection details
config = {
    'user': 'root',       # Your MySQL username
    'password': 'LZ#amhe!32',   # Your MySQL password
    'host': '127.0.0.1',           # The host where MySQL server is running
    'database': 'fyp',   # The database name where you want to create tables
    'raise_on_warnings': True
}
# Establish a connection to the MySQL database
cnx = mysql.connector.connect(**config)
cursor = cnx.cursor()
try:
    # SQL statement to create the Countries table
    create_countries_table = """
    CREATE TABLE IF NOT EXISTS Countries (
        country_id INT AUTO_INCREMENT PRIMARY KEY,
        country_name VARCHAR(255) NOT NULL,
        country_code VARCHAR(10) NOT NULL
    );
    """

    # SQL statement to create the Data Sources table
    create_data_sources_table = """
    CREATE TABLE IF NOT EXISTS Data_Sources (
        source_id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        website VARCHAR(255)
    );
    """

    # SQL statement to create the Birth Rate table
    create_birth_rate_table = """
    CREATE TABLE IF NOT EXISTS Birth_Rate (
        data_id INT AUTO_INCREMENT PRIMARY KEY,
        country_id INT NOT NULL,
        source_id INT NOT NULL,
        year INT NOT NULL,
        birth_rate FLOAT NOT NULL,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        FOREIGN KEY (country_id) REFERENCES Countries(country_id),
        FOREIGN KEY (source_id) REFERENCES Data_Sources(source_id)
    );
    """

    # SQL statement to create the total Fertility Rate table
    create_fertility_rate_table = """
    CREATE TABLE IF NOT EXISTS Fertility_Rate (
        data_id INT AUTO_INCREMENT PRIMARY KEY,
        country_id INT NOT NULL,
        source_id INT NOT NULL,
        year INT NOT NULL,
        Fertility_rate FLOAT NOT NULL,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        FOREIGN KEY (country_id) REFERENCES Countries(country_id),
        FOREIGN KEY (source_id) REFERENCES Data_Sources(source_id)
    );
    """
    
    # SQL statement to create the Death Rate table
    create_death_rate_table = """
    CREATE TABLE IF NOT EXISTS Death_Rate (
        data_id INT AUTO_INCREMENT PRIMARY KEY,
        country_id INT NOT NULL,
        source_id INT NOT NULL,
        year INT NOT NULL,
        death_rate FLOAT NOT NULL,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        FOREIGN KEY (country_id) REFERENCES Countries(country_id),
        FOREIGN KEY (source_id) REFERENCES Data_Sources(source_id)
    );
    """

    # SQL statement to create the Population table
    create_population_table = """
    CREATE TABLE IF NOT EXISTS Population (
        data_id INT AUTO_INCREMENT PRIMARY KEY,
        country_id INT NOT NULL,
        source_id INT NOT NULL,
        year INT NOT NULL,
        population BIGINT NOT NULL,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        FOREIGN KEY (country_id) REFERENCES Countries(country_id),
        FOREIGN KEY (source_id) REFERENCES Data_Sources(source_id)
    );
    """

    # Execute the SQL statements to create tables
    cursor.execute(create_countries_table)
    cursor.execute(create_data_sources_table)
    cursor.execute(create_birth_rate_table)
    cursor.execute(create_death_rate_table)
    cursor.execute(create_population_table)

    # Commit the changes to the database
    cnx.commit()
    print("Tables created successfully!")

except mysql.connector.Error as err:
    # Handle errors during the connection or execution
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password.")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist.")
    else:
        print(err)
finally:
    # Close the cursor and connection
    cursor.close()
    cnx.close()
