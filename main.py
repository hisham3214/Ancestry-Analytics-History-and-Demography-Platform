import mysql.connector
from mysql.connector import errorcode

# Replace with your MySQL connection details
config = {
    'user': 'root',       # Your MySQL username
    'password': 'LZ#amhe!32',   # Your MySQL password
    'host': '127.0.0.1',           # The host where MySQL server is running
    'database': 'fyp2',   # The database name where you want to create tables
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
    # SQL statement to create the total net migration table
    create_total_net_migration_table = """
    CREATE TABLE IF NOT EXISTS Total_Net_Migration (
        data_id INT AUTO_INCREMENT PRIMARY KEY,
        country_id INT NOT NULL,
        source_id INT NOT NULL,
        year INT NOT NULL,
        net_migration FLOAT NOT NULL,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        FOREIGN KEY (country_id) REFERENCES Countries(country_id),
        FOREIGN KEY (source_id) REFERENCES Data_Sources(source_id)
    );
    """
        # SQL statement to create the crude rate of net migration table
    create_crude_net_migration_rate_table = """
    CREATE TABLE IF NOT EXISTS Crude_Net_Migration_Rate (
        data_id INT AUTO_INCREMENT PRIMARY KEY,
        country_id INT NOT NULL,
        source_id INT NOT NULL,
        year INT NOT NULL,
        migration_rate FLOAT NOT NULL,
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
    # SQL statement to create the Sex Ratio at Birth table
    create_sex_ratio_at_birth_table = """
    CREATE TABLE IF NOT EXISTS Sex_Ratio_At_Birth (
        data_id INT AUTO_INCREMENT PRIMARY KEY,
        country_id INT NOT NULL,
        source_id INT NOT NULL,
        year INT NOT NULL,
        -- Ratio is typically expressed as males per female (e.g., 1.05, 1.06, etc.)
        sex_ratio_at_birth FLOAT NOT NULL,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        FOREIGN KEY (country_id) REFERENCES Countries(country_id),
        FOREIGN KEY (source_id) REFERENCES Data_Sources(source_id)
    );
    """

    # SQL statement to create the Sex Ratio of Total Population table
    create_sex_ratio_total_population_table = """
    CREATE TABLE IF NOT EXISTS Sex_Ratio_Total_Population (
        data_id INT AUTO_INCREMENT PRIMARY KEY,
        country_id INT NOT NULL,
        source_id INT NOT NULL,
        year INT NOT NULL,
        -- This is the ratio of total males to total females in the population
        sex_ratio FLOAT NOT NULL,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        FOREIGN KEY (country_id) REFERENCES Countries(country_id),
        FOREIGN KEY (source_id) REFERENCES Data_Sources(source_id)
    );
    """
    #SQL statement to create the "Median Age of Population" table
    create_median_age_table = """
    CREATE TABLE IF NOT EXISTS Median_Age (
        data_id INT AUTO_INCREMENT PRIMARY KEY,
        country_id INT NOT NULL,
        source_id INT NOT NULL,
        year INT NOT NULL,
        -- This is the ratio of total males to total females in the population
        age FLOAT NOT NULL,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        FOREIGN KEY (country_id) REFERENCES Countries(country_id),
        FOREIGN KEY (source_id) REFERENCES Data_Sources(source_id)
    );
    """

    #SQL statement to create the life expectancy at birth by sex table
    create_life_expectancy_at_birth_table = """
            CREATE TABLE IF NOT EXISTS life_expectancy_at_birth_by_sex (
                id INT AUTO_INCREMENT PRIMARY KEY,
                country_id INT NOT NULL,
                source_id INT NOT NULL,
                year VARCHAR(10) NOT NULL,
                sex_id INT,
                sex VARCHAR(50),
                life_expectancy FLOAT NOT NULL,
                last_updated DATETIME NOT NULL,
                FOREIGN KEY (country_id) REFERENCES Countries(country_id),
                FOREIGN KEY (source_id) REFERENCES Data_Sources(source_id)
    );
    """
    #SQL statement to create the infant mortality rate by sex table
    create_Infant_Mortality_Rate_By_Sex_table = """
            CREATE TABLE IF NOT EXISTS Infant_Mortality_Rate_By_Sex (
                id INT AUTO_INCREMENT PRIMARY KEY,
                country_id INT NOT NULL,
                source_id INT NOT NULL,
                year VARCHAR(10) NOT NULL,
                sex_id INT,
                sex VARCHAR(50),
                infant_mortality_rate FLOAT NOT NULL,
                last_updated DATETIME NOT NULL,
                FOREIGN KEY (country_id) REFERENCES Countries(country_id),
                FOREIGN KEY (source_id) REFERENCES Data_Sources(source_id)
    );
    """
    #SQL statement to create the under 5 mortality rate by sex table
    create_Under_Five_Mortality_Rate_By_Sex_table = """
            CREATE TABLE IF NOT EXISTS Under_Five_Mortality_Rate_By_Sex (
                id INT AUTO_INCREMENT PRIMARY KEY,
                country_id INT NOT NULL,
                source_id INT NOT NULL,
                year VARCHAR(10) NOT NULL,
                sex_id INT,
                sex VARCHAR(50),
                mortality_rate FLOAT NOT NULL,
                last_updated DATETIME NOT NULL,
                FOREIGN KEY (country_id) REFERENCES Countries(country_id),
                FOREIGN KEY (source_id) REFERENCES Data_Sources(source_id)
    );
    """
    # SQL statement to create the population by sex table
    create_population_by_sex_table = """
    CREATE TABLE IF NOT EXISTS Population_by_sex (
        id INT AUTO_INCREMENT PRIMARY KEY,
        country_id INT NOT NULL,
        source_id INT NOT NULL,
        year VARCHAR(10) NOT NULL,
        sex_id INT,
        sex VARCHAR(50),
        population FLOAT NOT NULL,
        last_updated DATETIME NOT NULL,
        FOREIGN KEY (country_id) REFERENCES Countries(country_id),
        FOREIGN KEY (source_id) REFERENCES Data_Sources(source_id)
    );
    """

    create_population_by_age_table = """
    CREATE TABLE IF NOT EXISTS Population_By_Age_Group (
        id INT AUTO_INCREMENT PRIMARY KEY,
        country_id INT NOT NULL,
        source_id INT NOT NULL,
        year VARCHAR(10) NOT NULL,
        sex_id INT ,
        sex VARCHAR(50),
        age_group_id INT,
        age_group_label VARCHAR(20),
        age_start INT,
        age_end INT,
        population FLOAT NOT NULL,
        last_updated DATETIME NOT NULL,
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
    cursor.execute(create_fertility_rate_table)
    cursor.execute(create_total_net_migration_table)
    cursor.execute(create_crude_net_migration_rate_table)
    cursor.execute(create_sex_ratio_at_birth_table)
    cursor.execute(create_sex_ratio_total_population_table)
    cursor.execute(create_median_age_table)
    cursor.execute(create_life_expectancy_at_birth_table)
    cursor.execute(create_Under_Five_Mortality_Rate_By_Sex_table)
    cursor.execute(create_Infant_Mortality_Rate_By_Sex_table)
    cursor.execute(create_population_by_sex_table)
    cursor.execute(create_population_by_age_table)




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