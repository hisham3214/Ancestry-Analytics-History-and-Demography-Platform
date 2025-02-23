import requests
import mysql.connector
from mysql.connector import errorcode

# ========== MySQL Configuration ==========
config = {
    'user': 'root',              # Your MySQL username
    'password': 'LZ#amhe!32',    # Your MySQL password
    'host': '127.0.0.1',         # The host where MySQL server is running
    'database': 'fyp',           # The database name where you want to insert data
    'raise_on_warnings': True
}

# ========== 1) Fetch All Data from Eurostat ==========
def fetch_all_eurostat_population():
    """
    Fetches the entire dataset for freq=A, sex=T, age=TOTAL, unit=NR
    from the demo_pjangroup table at Eurostat.

    Returns the JSON with dimension & value sections:
        {
          "dimension": {...},
          "value": { "0": 123, "1": 456, ... },
          ...
        }
    or raises an exception if the request fails.
    """
    base_url = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/demo_pjangroup"
    params = {
        "format": "JSON",
        "freq": "A",        # Annual
        "sex": "T",         # Total
        "age": "TOTAL",     # All ages combined
        "unit": "NR"        # Number of persons
        # no "geo" or "time" => means "ALL"
    }

    response = requests.get(base_url, params=params)
    response.raise_for_status()  # Will raise an HTTPError if not 200
    data = response.json()

    # Check if we got anything back
    if "value" not in data or not data["value"]:
        print("No population data returned from Eurostat for TOTAL population.")
        return None

    return data

# ========== 2) Parse and Insert the Data into MySQL ==========
def insert_all_countries_years(data):
    """
    Given the JSON 'data' returned from Eurostat, loop over all
    (country, year) pairs in the dimension, extract the population,
    and insert into the Population table. Only 'sex=T' and 'age=TOTAL'.

    The dimension order is typically: [freq, unit, sex, age, geo, time].
    Since freq=1, unit=1, sex=1, age=1, we only vary over geo & time.
    Thus index = geoIdx * timeSize + timeIdx in the 'value' dictionary.
    """
    dims = data["dimension"]
    size = data["size"]  # e.g. [1, 1, 1, 1, geoSize, timeSize]

    # Indices in 'size'
    geo_size = size[4]
    time_size = size[5]

    # Maps from country-code -> geoIdx
    geo_index = dims["geo"]["category"]["index"]    # e.g. {"CH": 0, "DE": 1, ...}
    geo_label = dims["geo"]["category"]["label"]    # e.g. {"CH": "Switzerland", "DE": "Germany", ...}

    # Maps from year-string -> timeIdx
    time_index = dims["time"]["category"]["index"]  # e.g. {"2023": 0, "2022": 1, ...}
    time_label = dims["time"]["category"]["label"]  # e.g. {"2023": "2023", "2022": "2022", ...}

    values = data["value"]  # dict of flattened index -> population

    # 1) Connect to MySQL
    try:
        cnx = mysql.connector.connect(**config)
        cursor = cnx.cursor()

        # 2) Prepare queries (we'll re-use them)
        # Find (or create) a 'Eurostat' source
        source_id = get_or_insert_datasource(cursor, "Eurostat", "https://ec.europa.eu/eurostat")

        # For each geo-code and year, compute the flattened index -> population
        for country_code, g_idx in geo_index.items():
            country_name = geo_label[country_code]

            # Make sure the Countries table has an entry
            country_id = get_or_insert_country(cursor, country_name, country_code)

            for year_str, t_idx in time_index.items():
                # Flattened index:
                # freq=0, unit=0, sex=0, age=0 => all are singletons => offset=0
                # geoIdx = g_idx, timeIdx = t_idx
                flat_idx = g_idx * time_size + t_idx

                # Get the population value if it exists
                pop_val = values.get(str(flat_idx))
                if pop_val is None:
                    # Means no data for that combination
                    continue

                # Convert year_str to an integer (Eurostat labels are plain strings like "2023")
                year_int = int(year_str)

                # Insert into the 'Population' table
                insert_population(cursor, country_id, source_id, year_int, pop_val)

        # Commit all
        cnx.commit()
        print("All data inserted successfully.")

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password.")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist.")
        else:
            print(err)
    finally:
        cursor.close()
        cnx.close()


# ---------- Helper: get_or_insert_country ----------
def get_or_insert_country(cursor, country_name, country_code):
    """
    Returns the country_id from the Countries table if it exists,
    otherwise inserts a new record and returns its ID.
    """
    query = """
        SELECT country_id
        FROM Countries
        WHERE country_code = %s
        LIMIT 1
    """

    two_to_three_letter_map = {
    "AF": "AFG",  # Afghanistan
    "AL": "ALB",  # Albania
    "DZ": "DZA",  # Algeria
    "AS": "ASM",  # American Samoa
    "AD": "AND",  # Andorra
    "AO": "AGO",  # Angola
    "AI": "AIA",  # Anguilla
    "AQ": "ATA",  # Antarctica
    "AG": "ATG",  # Antigua and Barbuda
    "AR": "ARG",  # Argentina
    "AM": "ARM",  # Armenia
    "AW": "ABW",  # Aruba
    "AU": "AUS",  # Australia
    "AT": "AUT",  # Austria
    "AZ": "AZE",  # Azerbaijan
    "BS": "BHS",  # Bahamas
    "BH": "BHR",  # Bahrain
    "BD": "BGD",  # Bangladesh
    "BB": "BRB",  # Barbados
    "BY": "BLR",  # Belarus
    "BE": "BEL",  # Belgium
    "BZ": "BLZ",  # Belize
    "BJ": "BEN",  # Benin
    "BM": "BMU",  # Bermuda
    "BT": "BTN",  # Bhutan
    "BO": "BOL",  # Bolivia
    "BQ": "BES",  # Bonaire, Sint Eustatius and Saba
    "BA": "BIH",  # Bosnia and Herzegovina
    "BW": "BWA",  # Botswana
    "BV": "BVT",  # Bouvet Island
    "BR": "BRA",  # Brazil
    "IO": "IOT",  # British Indian Ocean Territory
    "BN": "BRN",  # Brunei Darussalam
    "BG": "BGR",  # Bulgaria
    "BF": "BFA",  # Burkina Faso
    "BI": "BDI",  # Burundi
    "CV": "CPV",  # Cabo Verde
    "KH": "KHM",  # Cambodia
    "CM": "CMR",  # Cameroon
    "CA": "CAN",  # Canada
    "KY": "CYM",  # Cayman Islands
    "CF": "CAF",  # Central African Republic
    "TD": "TCD",  # Chad
    "CL": "CHL",  # Chile
    "CN": "CHN",  # China
    "CX": "CXR",  # Christmas Island
    "CC": "CCK",  # Cocos (Keeling) Islands
    "CO": "COL",  # Colombia
    "KM": "COM",  # Comoros
    "CD": "COD",  # Democratic Republic of the Congo
    "CG": "COG",  # Republic of the Congo
    "CK": "COK",  # Cook Islands
    "CR": "CRI",  # Costa Rica
    "HR": "HRV",  # Croatia
    "CU": "CUB",  # Cuba
    "CW": "CUW",  # Curaçao
    "CY": "CYP",  # Cyprus
    "CZ": "CZE",  # Czechia
    "CI": "CIV",  # Côte d'Ivoire
    "DK": "DNK",  # Denmark
    "DJ": "DJI",  # Djibouti
    "DM": "DMA",  # Dominica
    "DO": "DOM",  # Dominican Republic
    "EC": "ECU",  # Ecuador
    "EG": "EGY",  # Egypt
    "SM": "SMR",  # San Marino
    "SV": "SLV",  # El Salvador
    "GQ": "GNQ",  # Equatorial Guinea
    "ER": "ERI",  # Eritrea
    "EE": "EST",  # Estonia
    "SZ": "SWZ",  # Eswatini
    "ET": "ETH",  # Ethiopia
    "FI": "FIN",  # Finland
    "FR": "FRA",  # France
    "GE": "GEO",  # Georgia
    "DE": "DEU",  # Germany
    "GH": "GHA",  # Ghana
    "GR": "GRC",  # Greece
    "HU": "HUN",  # Hungary
    "IS": "ISL",  # Iceland
    "IN": "IND",  # India
    "ID": "IDN",  # Indonesia
    "IR": "IRN",  # Iran
    "IQ": "IRQ",  # Iraq
    "IE": "IRL",  # Ireland
    "IL": "ISR",  # Israel
    "IT": "ITA",  # Italy
    "JP": "JPN",  # Japan
    "JO": "JOR",  # Jordan
    "KZ": "KAZ",  # Kazakhstan
    "KE": "KEN",  # Kenya
    "KW": "KWT",  # Kuwait
    "LV": "LVA",  # Latvia
    "LB": "LBN",  # Lebanon
    "LT": "LTU",  # Lithuania
    "LU": "LUX",  # Luxembourg
    "MT": "MLT",  # Malta
    "MX": "MEX",  # Mexico
    "MD": "MDA",  # Moldova
    "MC": "MCO",  # Monaco
    "ME": "MNE",  # Montenegro
    "MA": "MAR",  # Morocco
    "NL": "NLD",  # Netherlands
    "NZ": "NZL",  # New Zealand
    "NO": "NOR",  # Norway
    "PK": "PAK",  # Pakistan
    "PL": "POL",  # Poland
    "PT": "PRT",  # Portugal
    "QA": "QAT",  # Qatar
    "RO": "ROU",  # Romania
    "RU": "RUS",  # Russia
    "SA": "SAU",  # Saudi Arabia
    "RS": "SRB",  # Serbia
    "SG": "SGP",  # Singapore
    "SK": "SVK",  # Slovakia
    "SI": "SVN",  # Slovenia
    "ZA": "ZAF",  # South Africa
    "KR": "KOR",  # South Korea
    "ES": "ESP",  # Spain
    "SE": "SWE",  # Sweden
    "CH": "CHE",  # Switzerland
    "TH": "THA",  # Thailand
    "TR": "TUR",  # Türkiye
    "UA": "UKR",  # Ukraine
    "AE": "ARE",  # United Arab Emirates
    "GB": "GBR",  # United Kingdom
    "UK": "GBR",  # United Kingdom
    "US": "USA",  # United States
    "VN": "VNM",  # Vietnam
    "YE": "YEM",  # Yemen
    "ZM": "ZMB",  # Zambia
    "ZW": "ZWE",  # Zimbabwe
    "XK": "XKX",   # Kosovo
    "MK": "MKD",   # North Macedonia
    "LI": "LIE",   # Liechtenstein
    "EL": "GRC"   # Greece
    }


    if country_code in two_to_three_letter_map:
        country_code = two_to_three_letter_map[country_code]

    cursor.execute(query, (country_code,))
    row = cursor.fetchone()
    if row:
        return row[0]

    # Insert new country
    insert_sql = """
        INSERT INTO Countries (country_name, country_code)
        VALUES (%s, %s)
    """
    cursor.execute(insert_sql, (country_name, country_code))
    return cursor.lastrowid


# ---------- Helper: get_or_insert_datasource ----------
def get_or_insert_datasource(cursor, source_name, website):
    """
    Returns the source_id if the data source exists,
    otherwise inserts a new record into Data_Sources.
    """
    query = """
        SELECT source_id
        FROM Data_Sources
        WHERE name = %s
        LIMIT 1
    """
    cursor.execute(query, (source_name,))
    row = cursor.fetchone()
    if row:
        return row[0]

    # Insert new source
    insert_sql = """
        INSERT INTO Data_Sources (name, website)
        VALUES (%s, %s)
    """
    cursor.execute(insert_sql, (source_name, website))
    return cursor.lastrowid


# ---------- Helper: insert_population ----------
def insert_population(cursor, country_id, source_id, year_int, population):
    """
    Inserts a row into the Population table.
    """
    insert_sql = """
        INSERT INTO Population (country_id, source_id, year, population)
        VALUES (%s, %s, %s, %s)
    """
    cursor.execute(insert_sql, (country_id, source_id, year_int, population))


# ========== Main ==========
if __name__ == "__main__":
    data = fetch_all_eurostat_population()
    if data:
        insert_all_countries_years(data)
