#!/usr/bin/env python3
# eurostat_loader.py  –  Fill the tables created in main.py with Eurostat data
# ---------------------------------------------------------------------------
import time, requests, pandas as pd, mysql.connector, datetime
from mysql.connector import errorcode

# ---------------------------------------------------------------------------
# 1.  MySQL connection (identical to main.py)
config = {
    "user":     "root",
    "password": "new_password",
    "host":     "127.0.0.1",
    "database": "fyp3",
    "raise_on_warnings": True,
}
cnx = mysql.connector.connect(**config)
cur = cnx.cursor(buffered=True)

# ---------------------------------------------------------------------------
# 2.  Helpers to create / look‑up source & countries
def get_or_create_source(name, url):
    cur.execute("SELECT source_id FROM Data_Sources WHERE name=%s", (name,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("INSERT INTO Data_Sources(name,website) VALUES(%s,%s)", (name, url))
    cnx.commit()
    return cur.lastrowid

EUROSTAT_ID = get_or_create_source("Eurostat",
                                   "https://ec.europa.eu/eurostat")

def get_or_create_country(code, label):
    cur.execute("SELECT country_id FROM Countries WHERE country_code=%s", (code,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("INSERT INTO Countries(country_name,country_code) VALUES(%s,%s)",
                (label, code))
    cnx.commit()
    return cur.lastrowid

def bootstrap_countries():
    """Pull every geo code once from a lightweight dataset (population)."""
    url = ("https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"
           "tps00001?freq=A&time=2023")
    geo_labels = requests.get(url, timeout=30).json()["dimension"]["geo"]["category"]["label"]
    for code, label in geo_labels.items():
        get_or_create_country(code, label)

bootstrap_countries()
cur.execute("SELECT country_id, country_code FROM Countries")
COUNTRY_ID = {code: cid for cid, code in cur.fetchall()}

# Define age group IDs
AGE_GROUP_MAP = {
    "Y_LT5": 1,     # Less than 5 years
    "Y5-9": 2,      # 5-9 years
    "Y10-14": 3,    # 10-14 years
    "Y15-19": 4,    # 15-19 years
    "Y20-24": 5,    # 20-24 years
    "Y25-29": 6,    # 25-29 years
    "Y30-34": 7,    # 30-34 years
    "Y35-39": 8,    # 35-39 years
    "Y40-44": 9,    # 40-44 years
    "Y45-49": 10,   # 45-49 years
    "Y50-54": 11,   # 50-54 years
    "Y55-59": 12,   # 55-59 years
    "Y60-64": 13,   # 60-64 years
    "Y65-69": 14,   # 65-69 years
    "Y70-74": 15,   # 70-74 years
    "Y75-79": 16,   # 75-79 years
    "Y80-84": 17,   # 80-84 years
    "Y85-89": 18,   # 85-89 years
    "Y90-94": 19,   # 90-94 years
    "Y_GE95": 20,   # 95 years or over
    "TOTAL": 0      # Total population
}

# Add a cache for sex ratio calculations
row_cache = {}

# ---------------------------------------------------------------------------
# 3.  Dataset → table mapping
JOBS = {
    # Existing entries
    "Birth_Rate":                  ("demo_gind", "GBIRTHRT",      "birth_rate"),
    "Death_Rate":                  ("demo_gind", "GDEATHRT",      "death_rate"),
    "Total_Net_Migration":         ("demo_gind", "CNMIGRAT",      "net_migration"),
    "Crude_Net_Migration_Rate":    ("demo_gind", "CNMIGRATRT",    "migration_rate"),
    "Population":                  ("tps00001",  None,            "population"),
    "Fertility_Rate":              ("tps00199",  None,            "Fertility_rate"),
    "Median_Age":                  ("demo_pjanind", "MEDAGEPOP",  "age"),
    "life_expectancy_at_birth_by_sex": ("tps00205",  None,        "life_expectancy"),
    "Infant_Mortality_Rate_By_Sex":    ("enps_demo_infant", None, "infant_mortality_rate"),
    "Under_Five_Mortality_Rate_By_Sex":("enps_demo_under5", None, "mortality_rate"),
    "Population_by_sex":           ("demo_pjan", None,            "population"),
    
    # New entries
    "Sex_Ratio_At_Birth":          ("demo_fasec", None,           "sex_ratio"),
    "Sex_Ratio_Total_Population":  ("demo_pjan", None,            "sex_ratio"),
}

# ---------------------------------------------------------------------------
# 4.  SQL templates
INS = {
    "Birth_Rate":
        "INSERT IGNORE INTO Birth_Rate(country_id,source_id,year,birth_rate)"
        " VALUES(%s,%s,%s,%s)",
    "Death_Rate":
        "INSERT IGNORE INTO Death_Rate(country_id,source_id,year,death_rate)"
        " VALUES(%s,%s,%s,%s)",
    "Total_Net_Migration":
        "INSERT IGNORE INTO Total_Net_Migration(country_id,source_id,year,net_migration)"
        " VALUES(%s,%s,%s,%s)",
    "Crude_Net_Migration_Rate":
        "INSERT IGNORE INTO Crude_Net_Migration_Rate(country_id,source_id,year,migration_rate)"
        " VALUES(%s,%s,%s,%s)",
    "Population":
        "INSERT IGNORE INTO Population(country_id,source_id,year,population)"
        " VALUES(%s,%s,%s,%s)",
    "Fertility_Rate":
        "INSERT IGNORE INTO Fertility_Rate(country_id,source_id,year,Fertility_rate)"
        " VALUES(%s,%s,%s,%s)",
    "Median_Age":
        "INSERT IGNORE INTO Median_Age(country_id,source_id,year,age)"
        " VALUES(%s,%s,%s,%s)",
    "life_expectancy_at_birth_by_sex":
        "INSERT IGNORE INTO life_expectancy_at_birth_by_sex(country_id,source_id,year,"
        "sex_id,sex,life_expectancy,last_updated) VALUES(%s,%s,%s,%s,%s,%s,%s)",
    "Infant_Mortality_Rate_By_Sex":
        "INSERT IGNORE INTO Infant_Mortality_Rate_By_Sex(country_id,source_id,year,"
        "sex_id,sex,infant_mortality_rate) VALUES(%s,%s,%s,%s,%s,%s)",
    "Under_Five_Mortality_Rate_By_Sex":
        "INSERT IGNORE INTO Under_Five_Mortality_Rate_By_Sex(country_id,source_id,year,"
        "sex_id,sex,mortality_rate) VALUES(%s,%s,%s,%s,%s,%s)",
    "Population_by_sex":
        "INSERT IGNORE INTO Population_by_sex(country_id,source_id,year,sex_id,sex,population,last_updated)"
        " VALUES(%s,%s,%s,%s,%s,%s,%s)",
    
    # New entries
    "Sex_Ratio_At_Birth":
        "INSERT IGNORE INTO Sex_Ratio_At_Birth(country_id,source_id,year,ratio)"
        " VALUES(%s,%s,%s,%s)",
    "Sex_Ratio_Total_Population":
        "INSERT IGNORE INTO Sex_Ratio_Total_Population(country_id,source_id,year,ratio)"
        " VALUES(%s,%s,%s,%s)",
}

# Add this function to check existing table structures
def check_table_structure(table):
    try:
        cur.execute(f"DESCRIBE {table}")
        columns = [col[0] for col in cur.fetchall()]
        print(f"{table} columns: {columns}")
        return columns
    except Exception as e:
        print(f"Error checking {table} structure: {e}")
        return []

# Add this function to get available years for a dataset
def get_available_years(dataset_code):
    """Query the API to get all available years for a given dataset."""
    try:
        # Query the API with minimal parameters to get metadata
        url = f"https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{dataset_code}?format=JSON"
        js = requests.get(url, timeout=60).json()
        
        # Extract time dimension info
        if "dimension" in js and "time" in js["dimension"]:
            available_years = list(js["dimension"]["time"]["category"]["index"].keys())
            available_years.sort()  # Sort chronologically
            print(f"Dataset {dataset_code} has {len(available_years)} years available ({min(available_years)}-{max(available_years)})")
            return available_years
        else:
            print(f"Could not find time dimension in dataset {dataset_code}")
            return test_years  # Fall back to test years
    except Exception as e:
        print(f"Error getting years for {dataset_code}: {e}")
        return test_years  # Fall back to test years

# ---------------------------------------------------------------------------
# 5.  Utility: fetch dataset safely
YEARS = [str(year) for year in range(2000, 2024)]

def fetch_json(url):
    while True:
        js = requests.get(url, timeout=60).json()
        if isinstance(js, dict) and js.get("warning", {}).get("status") == 413:
            print("   ↪ queued by Eurostat – waiting 10 s …")
            time.sleep(10)
            continue
        
        # Handle error responses properly
        if isinstance(js, dict) and "error" in js:
            if isinstance(js["error"], dict):
                print(f"Error: {js['error'].get('code')} - {js['error'].get('label')}")
            elif isinstance(js["error"], list):
                print(f"Error: {js['error']}")
            else:
                print(f"Unknown error format: {js['error']}")
            # You might want to raise an exception or handle the error differently
            raise Exception(f"API error when fetching {url}")
            
        return js

def json_to_df(js):
    dims, ids, sizes = js["dimension"], js["id"], js["size"]
    cats = [list(dims[d]["category"]["index"].keys()) for d in ids]
    lbls = [dims[d]["category"]["label"]            for d in ids]
    recs = []
    for flat, val in js["value"].items():
        idxs = []; n = int(flat)
        for sz in reversed(sizes):
            idxs.append(n % sz); n //= sz
        idxs = list(reversed(idxs))
        row = {}
        for dim, pos, lab in zip(ids, idxs, lbls):
            code = cats[ids.index(dim)][pos]
            row[dim] = code
            if dim in ("geo", "sex"):
                row[f"{dim}_label"] = lab[code]
        row["value"] = val
        recs.append(row)
    return pd.DataFrame.from_records(recs)

row_cache = {}

def insert_rows(df, table, valcol):
    current_time = datetime.datetime.now()
    for _, r in df.iterrows():
        geo = r["geo"]
        if geo not in COUNTRY_ID:
            continue
        yr = int(r["time"])
        
        # Existing conditions
        if table == "life_expectancy_at_birth_by_sex":
            sex = r.get("sex", "T")
            cur.execute(INS[table],
                        (COUNTRY_ID[geo], EUROSTAT_ID, yr,
                         {"M":1,"F":2,"T":0}.get(sex,0),
                         r.get("sex_label", sex),
                         float(r["value"]),
                         current_time))
        elif table == "Population_by_sex":
            sex = r.get("sex", "T")
            cur.execute(INS[table],
                        (COUNTRY_ID[geo], EUROSTAT_ID, yr,
                         {"M":1,"F":2,"T":0}.get(sex,0),
                         r.get("sex_label", sex),
                         float(r["value"]),
                         current_time))
        elif table in ("Infant_Mortality_Rate_By_Sex", "Under_Five_Mortality_Rate_By_Sex"):
            sex = r.get("sex", "T")
            cur.execute(INS[table],
                        (COUNTRY_ID[geo], EUROSTAT_ID, yr,
                         {"M":1,"F":2,"T":0}.get(sex,0),
                         r.get("sex_label", sex),
                         float(r["value"])))
        
        # New conditions
        elif table == "Sex_Ratio_At_Birth":
            cur.execute(INS[table],
                        (COUNTRY_ID[geo], EUROSTAT_ID, yr, float(r["value"])))
        elif table == "Sex_Ratio_Total_Population":
            # This requires special handling - we'll calculate the sex ratio
            if "sex" in r and r["sex"] == "M" and "M_pop" not in row_cache.get(geo, {}).get(yr, {}):
                row_cache.setdefault(geo, {}).setdefault(yr, {})["M_pop"] = float(r["value"])
            elif "sex" in r and r["sex"] == "F" and "F_pop" not in row_cache.get(geo, {}).get(yr, {}):
                row_cache.setdefault(geo, {}).setdefault(yr, {})["F_pop"] = float(r["value"])
                
            # If we have both male and female populations for this geo/year, calculate ratio
            if geo in row_cache and yr in row_cache[geo] and "M_pop" in row_cache[geo][yr] and "F_pop" in row_cache[geo][yr]:
                m_pop = row_cache[geo][yr]["M_pop"]
                f_pop = row_cache[geo][yr]["F_pop"]
                if f_pop > 0:  # Avoid division by zero
                    sex_ratio = (m_pop / f_pop) * 100
                    cur.execute(INS[table],
                                (COUNTRY_ID[geo], EUROSTAT_ID, yr, sex_ratio))
        else:
            cur.execute(INS[table],
                        (COUNTRY_ID[geo], EUROSTAT_ID, yr, float(r["value"])))
    cnx.commit()

# ---------------------------------------------------------------------------
# 6.  Main loop - process ALL tables with all available years
# Use all tables from JOBS dictionary, processing one year at a time
for table, (ds, indic, valcol) in JOBS.items():
    print(f"\n=== Processing {table} ===")
    base = f"https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{ds}?freq=A"
    if indic:
        base += f"&indic_de={indic}"
    
    # Apply extra filters based on table type
    if table == "Population_by_sex" or table == "Sex_Ratio_Total_Population":
        base += "&age=TOTAL"
        
    # Reset row cache for tables that need it
    if table == "Sex_Ratio_Total_Population":
        row_cache = {geo: {} for geo in COUNTRY_ID}  # Initialize cache
    
    # Get all available years for this dataset
    try:
        available_years = get_available_years(ds)
        print(f"Processing {len(available_years)} years for {table}")
        
        # Process each year individually
        for i, year in enumerate(available_years):
            url = f"{base}&time={year}"
            print(f"Downloading {ds} {year} for {table} ({i+1}/{len(available_years)})...")
            
            try:
                js = fetch_json(url)
                df = json_to_df(js)
                print(f"  Retrieved {len(df)} data points")
                insert_rows(df, table, valcol)
                time.sleep(1)  # Be kind to the API
            except Exception as e:
                print(f"Error processing {table} for {year}: {e}")
                continue
    except Exception as e:
        print(f"Failed to process table {table}: {e}")
        continue

# ---------------------------------------------------------------------------
cur.close(); cnx.close()
print("✓ All available years processed for all tables.")
