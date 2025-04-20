#!/usr/bin/env python3
# Optimized Eurostat data fetcher - Uses only one optimal dataset per indicator
import time, requests, pandas as pd, mysql.connector, datetime, os
from mysql.connector import errorcode

# ISO-2 to ISO-3 country code mapping
ISO2_TO_ISO3 = {
    'AT': 'AUT', 'BE': 'BEL', 'BG': 'BGR', 'HR': 'HRV', 'CY': 'CYP', 
    'CZ': 'CZE', 'DK': 'DNK', 'EE': 'EST', 'FI': 'FIN', 'FR': 'FRA', 
    'DE': 'DEU', 'EL': 'GRC', 'GR': 'GRC', 'HU': 'HUN', 'IE': 'IRL', 
    'IT': 'ITA', 'LV': 'LVA', 'LT': 'LTU', 'LU': 'LUX', 'MT': 'MLT', 
    'NL': 'NLD', 'PL': 'POL', 'PT': 'PRT', 'RO': 'ROU', 'SK': 'SVK', 
    'SI': 'SVN', 'ES': 'ESP', 'SE': 'SWE', 'UK': 'GBR', 'GB': 'GBR',
    'IS': 'ISL', 'LI': 'LIE', 'NO': 'NOR', 'CH': 'CHE', 'ME': 'MNE', 
    'MK': 'MKD', 'AL': 'ALB', 'RS': 'SRB', 'TR': 'TUR', 'BA': 'BIH',
    'XK': 'XKX', 'MD': 'MDA', 'UA': 'UKR', 'BY': 'BLR', 'RU': 'RUS',
    'AM': 'ARM', 'AZ': 'AZE', 'GE': 'GEO'
}

# ---------------------------------------------------------------------------
# 1. MySQL connection and logging setup (unchanged)
config = {
    "user":     "root",
    "password": "new_password",
    "host":     "127.0.0.1", 
    "database": "fyp3",
    "raise_on_warnings": True,
}
cnx = mysql.connector.connect(**config)
cur = cnx.cursor(buffered=True)

LOG_DIR = "eurostat_logs"
os.makedirs(LOG_DIR, exist_ok=True)
log_file = f"{LOG_DIR}/eurostat_optimized_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

def log_message(message):
    """Log message to both console and file"""
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    formatted = f"[{timestamp}] {message}"
    print(formatted)
    with open(log_file, "a") as f:
        f.write(formatted + "\n")

# ---------------------------------------------------------------------------
# 2. Source & country helpers (unchanged)
def get_or_create_source(name, url):
    cur.execute("SELECT source_id FROM Data_Sources WHERE name=%s", (name,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("INSERT INTO Data_Sources(name,website) VALUES(%s,%s)", (name, url))
    cnx.commit()
    return cur.lastrowid

EUROSTAT_ID = get_or_create_source("Eurostat", "https://ec.europa.eu/eurostat")

def ensure_countries_exist():
    """Check if Countries table has data and populate it if empty"""
    # Check if table is empty
    cur.execute("SELECT COUNT(*) FROM Countries")
    count = cur.fetchone()[0]
    
    if count == 0:
        log_message("Countries table is empty. Populating with standard country codes...")
        
        # First get country names from Eurostat
        country_names = {}
        try:
            url = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/demo_pjan?freq=A&sex=T&age=TOTAL&time=2023"
            response = requests.get(url, timeout=60)
            data = response.json()
            country_labels = data["dimension"]["geo"]["category"]["label"]
            
            # Map ISO2 to country names
            for iso2, name in country_labels.items():
                if iso2 in ISO2_TO_ISO3:
                    country_names[ISO2_TO_ISO3[iso2]] = name
        except Exception as e:
            log_message(f"Warning: Could not fetch country names: {e}")
        
        # Add countries that might be missing
        standard_names = {
            'AUT': 'Austria', 'BEL': 'Belgium', 'BGR': 'Bulgaria', 'HRV': 'Croatia',
            'CYP': 'Cyprus', 'CZE': 'Czech Republic', 'DNK': 'Denmark', 'EST': 'Estonia',
            'FIN': 'Finland', 'FRA': 'France', 'DEU': 'Germany', 'GRC': 'Greece',
            'HUN': 'Hungary', 'IRL': 'Ireland', 'ITA': 'Italy', 'LVA': 'Latvia',
            'LTU': 'Lithuania', 'LUX': 'Luxembourg', 'MLT': 'Malta', 'NLD': 'Netherlands',
            'POL': 'Poland', 'PRT': 'Portugal', 'ROU': 'Romania', 'SVK': 'Slovakia',
            'SVN': 'Slovenia', 'ESP': 'Spain', 'SWE': 'Sweden', 'GBR': 'United Kingdom',
            'ISL': 'Iceland', 'LIE': 'Liechtenstein', 'NOR': 'Norway', 'CHE': 'Switzerland',
            'MNE': 'Montenegro', 'MKD': 'North Macedonia', 'ALB': 'Albania', 'SRB': 'Serbia',
            'TUR': 'Turkey', 'BIH': 'Bosnia and Herzegovina'
        }
        
        # Merge names from Eurostat with standard names, preferring Eurostat if available
        for iso3, name in standard_names.items():
            if iso3 not in country_names:
                country_names[iso3] = name
        
        # Insert countries
        for iso3, name in country_names.items():
            try:
                cur.execute("INSERT INTO Countries (country_name, country_code) VALUES (%s, %s)", 
                           (name, iso3))
            except Exception as e:
                log_message(f"Error adding country {name} ({iso3}): {e}")
        
        cnx.commit()
        log_message(f"Added {len(country_names)} countries to database.")
    else:
        log_message(f"Found {count} countries in database.")

def get_or_create_country(code, label):
    cur.execute("SELECT country_id FROM Countries WHERE country_code=%s", (code,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("INSERT INTO Countries(country_name,country_code) VALUES(%s,%s)",
                (label, code))
    cnx.commit()
    return cur.lastrowid

def build_country_mapping():
    """Build a mapping from Eurostat 2-letter codes to our database country IDs with improved matching"""
    # Get all countries in our database (fetch both name and code)
    cur.execute("SELECT country_id, country_name, country_code FROM Countries")
    db_countries = {}
    db_names = {}
    for cid, name, code in cur.fetchall():
        db_countries[code] = cid
        # Store lowercase name for case-insensitive matching
        db_names[name.lower()] = cid
    
    log_message(f"Found {len(db_countries)} countries in database")
    
    # Get Eurostat country labels
    url = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/demo_pjan?freq=A&sex=T&age=TOTAL&time=2023"
    eurostat_labels = {}
    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        data = response.json()
        eurostat_labels = data["dimension"]["geo"]["category"]["label"]
    except Exception as e:
        log_message(f"Warning: Could not fetch Eurostat country labels: {e}")
    
    # Build the mapping with multiple fallback strategies
    country_map = {}
    
    # 1. First try direct ISO2 to ISO3 mapping
    for iso2_code, iso3_code in ISO2_TO_ISO3.items():
        if iso3_code in db_countries:
            country_map[iso2_code] = db_countries[iso3_code]
    
    # 2. Then try direct ISO2 code (some databases store ISO2 codes)
    for iso2_code in ISO2_TO_ISO3:
        if iso2_code in db_countries and iso2_code not in country_map:
            country_map[iso2_code] = db_countries[iso2_code]
    
    # 3. Finally try name matching
    for iso2_code, label in eurostat_labels.items():
        if iso2_code not in country_map and label.lower() in db_names:
            country_map[iso2_code] = db_names[label.lower()]
    
    # Log detailed matching results
    log_message(f"Successfully mapped {len(country_map)}/{len(eurostat_labels)} Eurostat countries to database")
    
    # Print unmapped countries if we have serious mapping issues
    if len(country_map) < 10 and len(eurostat_labels) > 0:
        log_message("WARNING: Very few countries mapped - dumping database countries for debugging:")
        for code, cid in db_countries.items():
            log_message(f"  DB Country: ID={cid}, Code={code}")
        
        # Also show what we're trying to map from
        log_message("Eurostat countries we're trying to map:")
        for iso2, label in list(eurostat_labels.items())[:20]:  # First 20 only
            log_message(f"  {iso2} = {label} → {ISO2_TO_ISO3.get(iso2, 'N/A')}")
    
    # Filter out EU aggregate entries
    for key in list(country_map.keys()):
        if key.startswith(('EU', 'EA', 'EEA', 'EFTA')):
            del country_map[key]
    
    return country_map

# Ensure countries exist before building the country mapping
ensure_countries_exist()

# Build the country mapping
COUNTRY_ID = build_country_mapping()

# ---------------------------------------------------------------------------
# 3. Dataset metadata - all possible datasets for each indicator
DATASETS = {
    "Population": [
        {"code": "demo_pjan", "params": "&sex=T&age=TOTAL"},
        {"code": "tps00001", "params": ""},
        {"code": "demo_gind", "params": ""},
    ],
    "Population_by_sex": [
        {"code": "demo_pjan", "params": ""},
        {"code": "demo_pjangroup", "params": ""},
    ],
    "Birth_Rate": [
        {"code": "demo_gind", "params": "&indic_de=GBIRTHRT"},
        {"code": "demo_frate", "params": ""},
    ],
    "Death_Rate": [
        {"code": "demo_gind", "params": "&indic_de=GDEATHRT"},
        {"code": "demo_mrate", "params": ""},
    ],
    "Total_Net_Migration": [
        {"code": "demo_gind", "params": "&indic_de=CNMIGRAT"},
    ],
    "Crude_Net_Migration_Rate": [
        {"code": "demo_gind", "params": "&indic_de=CNMIGRATRT"},
    ],
    "Fertility_Rate": [
        {"code": "demo_find", "params": ""},
        {"code": "tps00199", "params": ""},
    ],
    "Median_Age": [
        {"code": "demo_pjanind", "params": "&indic_de=MEDAGEPOP"},
        {"code": "demo_pjangroup", "params": ""},
    ],
    "life_expectancy_at_birth_by_sex": [
        {"code": "demo_mlexpec", "params": ""},
        {"code": "tps00205", "params": ""},
    ],
    "Infant_Mortality_Rate_By_Sex": [
        {"code": "demo_minfind", "params": "&sex=T,M,F"},
    ],
    "Under_Five_Mortality_Rate_By_Sex": [
        {"code": "demo_minfind", "params": ""},
        {"code": "enps_demo_under5", "params": ""},
    ],
    "Sex_Ratio_At_Birth": [
        {"code": "demo_fasec", "params": "&unit=NR&sex=M"},
    ],
    "Sex_Ratio_Total_Population": [
        {"code": "demo_pjan", "params": ""},
    ],
}

# ---------------------------------------------------------------------------
# 4. SQL templates (unchanged)
INS = {
    "Population": "INSERT IGNORE INTO Population(country_id,source_id,year,population) VALUES(%s,%s,%s,%s)",
    "Population_by_sex": "INSERT IGNORE INTO Population_by_sex(country_id,source_id,year,sex_id,sex,population,last_updated) VALUES(%s,%s,%s,%s,%s,%s,%s)",
    "Birth_Rate": "INSERT IGNORE INTO Birth_Rate(country_id,source_id,year,birth_rate) VALUES(%s,%s,%s,%s)",
    "Death_Rate": "INSERT IGNORE INTO Death_Rate(country_id,source_id,year,death_rate) VALUES(%s,%s,%s,%s)",
    "Total_Net_Migration": "INSERT IGNORE INTO Total_Net_Migration(country_id,source_id,year,net_migration) VALUES(%s,%s,%s,%s)",
    "Crude_Net_Migration_Rate": "INSERT IGNORE INTO Crude_Net_Migration_Rate(country_id,source_id,year,migration_rate) VALUES(%s,%s,%s,%s)",
    "Fertility_Rate": "INSERT IGNORE INTO Fertility_Rate(country_id,source_id,year,Fertility_rate) VALUES(%s,%s,%s,%s)",
    "Median_Age": "INSERT IGNORE INTO Median_Age(country_id,source_id,year,age) VALUES(%s,%s,%s,%s)",
    "life_expectancy_at_birth_by_sex": "INSERT IGNORE INTO life_expectancy_at_birth_by_sex(country_id,source_id,year,sex_id,sex,life_expectancy,last_updated) VALUES(%s,%s,%s,%s,%s,%s,%s)",
    "Infant_Mortality_Rate_By_Sex": "INSERT IGNORE INTO Infant_Mortality_Rate_By_Sex(country_id,source_id,year,sex_id,sex,infant_mortality_rate,last_updated) VALUES(%s,%s,%s,%s,%s,%s,%s)",
    "Under_Five_Mortality_Rate_By_Sex": "INSERT IGNORE INTO Under_Five_Mortality_Rate_By_Sex(country_id,source_id,year,sex_id,sex,mortality_rate,last_updated) VALUES(%s,%s,%s,%s,%s,%s,%s)",
    "Sex_Ratio_At_Birth": "INSERT IGNORE INTO Sex_Ratio_At_Birth(country_id,source_id,year,sex_ratio_at_birth) VALUES(%s,%s,%s,%s)",
    "Sex_Ratio_Total_Population": "INSERT IGNORE INTO Sex_Ratio_Total_Population(country_id,source_id,year,sex_ratio) VALUES(%s,%s,%s,%s)",
}

# ---------------------------------------------------------------------------
# 5. Utility functions 
def get_available_years(dataset_code, params=""):
    """Query the API to get all available years for a given dataset."""
    try:
        url = f"https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{dataset_code}?format=JSON{params}"
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        js = response.json()
        
        if "dimension" in js and "time" in js["dimension"]:
            available_years = list(js["dimension"]["time"]["category"]["index"].keys())
            available_years.sort()  # Sort chronologically
            log_message(f"Dataset {dataset_code} has {len(available_years)} years: {min(available_years)}-{max(available_years)}")
            return available_years
        else:
            log_message(f"No time dimension found in {dataset_code}")
            return []
    except Exception as e:
        log_message(f"Error getting years for {dataset_code}: {e}")
        return []

def fetch_json(url, max_retries=3):
    """Fetch JSON data with retries and rate limiting"""
    retries = 0
    while retries < max_retries:
        try:
            response = requests.get(url, timeout=60)
            
            # Check for HTTP errors
            if response.status_code == 400:
                log_message(f"Bad Request error for URL: {url}")
                log_message(f"This usually means incorrect parameters or unsupported dataset combination")
                raise Exception(f"API returned 400 Bad Request - check parameters")
                
            response.raise_for_status()
            js = response.json()
            
            # Handle queued status from Eurostat
            if isinstance(js, dict) and js.get("warning", {}).get("status") == 413:
                log_message("   ↪ queued by Eurostat – waiting 15s...")
                time.sleep(15)
                retries += 1
                continue
            
            # Handle error responses
            if isinstance(js, dict) and "error" in js:
                error_msg = js["error"]
                if isinstance(error_msg, dict):
                    log_message(f"API Error: {error_msg.get('code')} - {error_msg.get('label')}")
                else:
                    log_message(f"API Error: {error_msg}")
                retries += 1
                time.sleep(5)  # Back off a bit
                continue
                
            return js
        
        except requests.exceptions.RequestException as e:
            log_message(f"Request failed: {e}")
            retries += 1
            time.sleep(5)
            
        except Exception as e:
            log_message(f"Unexpected error: {e}")
            retries += 1
            time.sleep(5)
    
    # If we get here, all retries failed
    raise Exception(f"Failed to fetch data after {max_retries} attempts")

def json_to_df(js):
    """Convert Eurostat JSON response to pandas DataFrame"""
    try:
        # Extract structure information
        dims = js["dimension"]
        ids = js["id"]
        sizes = js["size"]
        
        # Extract category information
        cats = [list(dims[d]["category"]["index"].keys()) for d in ids]
        lbls = [dims[d]["category"]["label"] for d in ids]
        
        # Create records from values
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
    except Exception as e:
        log_message(f"Error converting JSON to DataFrame: {e}")
        raise

def insert_rows(df, table, valcol):
    """Insert data into the database tables"""
    current_time = datetime.datetime.now()
    row_count = 0
    skipped_countries = set()  # Keep track of which countries were skipped
    
    for _, r in df.iterrows():
        try:
            geo = r["geo"]
            if geo not in COUNTRY_ID:
                skipped_countries.add(geo)
                continue
                
            yr = int(r["time"])
            
            # Handle different table types
            if table == "life_expectancy_at_birth_by_sex":
                sex = r.get("sex", "T")
                cur.execute(INS[table],
                            (COUNTRY_ID[geo], EUROSTAT_ID, yr,
                             {"M":1,"F":2,"T":0}.get(sex,0),
                             r.get("sex_label", sex),
                             float(r["value"]),
                             current_time))
                row_count += 1
                
            elif table == "Population_by_sex":
                sex = r.get("sex", "T")
                cur.execute(INS[table],
                            (COUNTRY_ID[geo], EUROSTAT_ID, yr,
                             {"M":1,"F":2,"T":0}.get(sex,0),
                             r.get("sex_label", sex),
                             float(r["value"]),
                             current_time))
                row_count += 1
                
            elif table in ("Infant_Mortality_Rate_By_Sex", "Under_Five_Mortality_Rate_By_Sex"):
                sex = r.get("sex", "T")
                cur.execute(INS[table],
                            (COUNTRY_ID[geo], EUROSTAT_ID, yr,
                             {"M":1,"F":2,"T":0}.get(sex,0),
                             r.get("sex_label", sex),
                             float(r["value"]),
                             current_time))  # Add timestamp
                row_count += 1
                
            elif table == "Sex_Ratio_Total_Population":
                # This requires special handling for calculating sex ratio
                if "sex" in r:
                    if r["sex"] == "M":
                        row_cache.setdefault(geo, {}).setdefault(yr, {})["M_pop"] = float(r["value"])
                    elif r["sex"] == "F":
                        row_cache.setdefault(geo, {}).setdefault(yr, {})["F_pop"] = float(r["value"])
                
                # Calculate ratio if we have both values
                if geo in row_cache and yr in row_cache[geo] and "M_pop" in row_cache[geo][yr] and "F_pop" in row_cache[geo][yr]:
                    m_pop = row_cache[geo][yr]["M_pop"]
                    f_pop = row_cache[geo][yr]["F_pop"]
                    if f_pop > 0:  # Avoid division by zero
                        sex_ratio = (m_pop / f_pop) * 100
                        cur.execute(INS[table],
                                    (COUNTRY_ID[geo], EUROSTAT_ID, yr, sex_ratio))
                        row_count += 1
            else:
                # Default case for most tables
                cur.execute(INS[table],
                            (COUNTRY_ID[geo], EUROSTAT_ID, yr, float(r["value"])))
                row_count += 1
                
        except Exception as e:
            log_message(f"Error inserting row for {geo}, {yr}: {e}")
    
    if skipped_countries:
        log_message(f"Skipped data for {len(skipped_countries)} unmapped countries: {', '.join(sorted(skipped_countries))}")
    
    cnx.commit()
    return row_count

def process_dataset(table, dataset_code, params=""):
    """Process a single dataset for all available years"""
    base_url = f"https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{dataset_code}?freq=A"
    
    # Add extra parameters
    if params:
        base_url += params
    
    # Get all available years
    available_years = get_available_years(dataset_code, params)
    if not available_years:
        log_message(f"No available years found for {dataset_code}")
        return 0
    
    log_message(f"Processing {len(available_years)} years for {table} from {dataset_code}")
    total_rows = 0
    
    # Process each year
    for i, year in enumerate(available_years):
        try:
            url = f"{base_url}&time={year}"
            log_message(f"Downloading {dataset_code} {year} for {table} ({i+1}/{len(available_years)})...")
            
            js = fetch_json(url)
            df = json_to_df(js)
            
            rows_inserted = insert_rows(df, table, table.lower())
            total_rows += rows_inserted
            log_message(f"  Retrieved {len(df)} data points, inserted {rows_inserted} rows")
            
            # Rate limiting
            time.sleep(1)
            
        except Exception as e:
            log_message(f"Error processing {table} for {year}: {e}")
            continue
    
    return total_rows

def find_best_dataset(table):
    """Find the dataset with the longest time span for a given table"""
    candidates = DATASETS[table]
    best_dataset = None
    best_span = 0
    
    for ds_info in candidates:
        code = ds_info["code"]
        params = ds_info["params"]
        
        years = get_available_years(code, params)
        if not years:
            continue
            
        span = len(years)
        if span > best_span:
            best_span = span
            best_dataset = ds_info
            
        # Give some time between API calls
        time.sleep(1)
    
    if best_dataset:
        log_message(f"Best dataset for {table}: {best_dataset['code']} with {best_span} years")
        return best_dataset
    else:
        log_message(f"No suitable dataset found for {table}")
        return None

# ---------------------------------------------------------------------------
# 6. Main execution
row_cache = {}  # Initialize cache for sex ratio calculations

log_message("=== Starting Optimized Eurostat Data Extraction ===")
log_message(f"Countries in database: {len(COUNTRY_ID)}")

# Process only the tables specified by the user (or all tables if none specified)
tables_to_process = list(DATASETS.keys())
log_message(f"Will process {len(tables_to_process)} tables")

# Find and use the best dataset for each table
for table in tables_to_process:
    log_message(f"\n=== Processing {table} ===")
    
    # Reset row cache for tables that need it
    if table == "Sex_Ratio_Total_Population":
        row_cache = {geo: {} for geo in COUNTRY_ID}
    
    # Find the best dataset for this table
    best_dataset = find_best_dataset(table)
    if not best_dataset:
        log_message(f"Skipping {table} - no suitable dataset found")
        continue
        
    # Process the table with the best dataset only
    try:
        dataset_code = best_dataset["code"]
        params = best_dataset["params"]
        
        rows = process_dataset(table, dataset_code, params)
        log_message(f"Added {rows} rows to {table} using {dataset_code}")
    except Exception as e:
        log_message(f"Error processing {table}: {e}")

# Close connections
cur.close()
cnx.close()
log_message("✓ Extraction complete - used optimal dataset for each indicator")
log_message(f"Log file saved to: {log_file}")