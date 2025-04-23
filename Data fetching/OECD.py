import pandas as pd
import mysql.connector
from datetime import datetime

# Load the dataset
csv_path = "Data fetching\OECD.ELS.SAE,DSD_POPULATION@DF_POP_HIST,1.0+all.csv"
df = pd.read_csv(csv_path)

# Drop rows with no population value
df = df.dropna(subset=["OBS_VALUE"])
df = df[df["Age"] == "Total"]

# MySQL config
config = {
    'user': 'root',
    'password': 'khalil_13579',
    'host': '127.0.0.1',
    'database': 'fyp',
    'raise_on_warnings': True
}

cnx = mysql.connector.connect(**config)
cursor = cnx.cursor()

# Insert Data Source
source_name = "OECD Population Dataset 3"
source_website = "https://www.oecd.org/"
cursor.execute("SELECT source_id FROM Data_Sources WHERE name = %s", (source_name,))
result = cursor.fetchone()
if result:
    source_id = result[0]
else:
    cursor.execute("INSERT INTO Data_Sources (name, website) VALUES (%s, %s)", (source_name, source_website))
    source_id = cursor.lastrowid
    cnx.commit()

# Insert countries if not exists
country_map = {}
for country_code, country_name in df[["REF_AREA", "Reference area"]].drop_duplicates().values:
    cursor.execute("SELECT country_id FROM Countries WHERE country_code = %s", (country_code,))
    result = cursor.fetchone()
    if result:
        country_map[country_code] = result[0]
    else:
        cursor.execute("INSERT INTO Countries (country_name, country_code) VALUES (%s, %s)", (country_name, country_code))
        country_map[country_code] = cursor.lastrowid
        cnx.commit()

# Step: Aggregate population by country, year, and sex
df_agg_sex = (
    df[df["SEX"].isin(["M", "F"])]
    .groupby(["REF_AREA", "TIME_PERIOD", "SEX", "Sex"])["OBS_VALUE"]
    .sum()
    .reset_index()
)

# Step: Insert aggregated data into Population_by_sex
for _, row in df_agg_sex.iterrows():
    country_code = row["REF_AREA"]
    year = int(row["TIME_PERIOD"])
    sex_code = row["SEX"]
    sex_label = row["Sex"]
    population_value = float(row["OBS_VALUE"])
    country_id = country_map[country_code]
    sex_id = 1 if sex_code == "M" else 2
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cursor.execute("""
        INSERT INTO Population_by_sex (country_id, source_id, year, sex_id, sex, population, last_updated)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (country_id, source_id, year, sex_id, sex_label, population_value, now))


# Derive and insert total population
df_sex_specific = df[df["SEX"].isin(["M", "F"])].copy()
aggregated = df_sex_specific.groupby(["REF_AREA", "TIME_PERIOD"])["OBS_VALUE"].sum().reset_index()

for _, row in aggregated.iterrows():
    country_code = row["REF_AREA"]
    year = int(row["TIME_PERIOD"])
    total_population = float(row["OBS_VALUE"])
    country_id = country_map[country_code]
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cursor.execute("""
        INSERT INTO Population (country_id, source_id, year, population, last_updated)
        VALUES (%s, %s, %s, %s, %s)
    """, (country_id, source_id, year, total_population, now))

cnx.commit()
cursor.close()
cnx.close()
print("Data inserted successfully.")
