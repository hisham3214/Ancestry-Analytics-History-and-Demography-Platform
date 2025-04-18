import mysql.connector
import pandas as pd
import numpy as np

# ------------------------------------------------------------------
# Config
# ------------------------------------------------------------------
WINDOW_YEARS = 5          # trailing window size
MIN_PERIODS  = 3          # observations required before trusting z‑score

db_config = {
    "user":     "root",
    "password": "LZ#amhe!32",
    "host":     "127.0.0.1",
    "database": "fyp2",
}

# ------------------------------------------------------------------
# Metadata for every table you want to process
# ------------------------------------------------------------------
tables_info = [
    # -------------- core tables --------------
    {"table_name": "Population",                     "id_col": "data_id", "value_col": "population",           "zscore_col": "population_zscore"},
    {"table_name": "Birth_Rate",                     "id_col": "data_id", "value_col": "birth_rate",           "zscore_col": "birth_rate_zscore"},
    {"table_name": "Death_Rate",                     "id_col": "data_id", "value_col": "death_rate",           "zscore_col": "death_rate_zscore"},
    {"table_name": "Fertility_Rate",                 "id_col": "data_id", "value_col": "fertility_rate",       "zscore_col": "fertility_rate_zscore"},
    {"table_name": "Total_Net_Migration",            "id_col": "data_id", "value_col": "net_migration",        "zscore_col": "net_migration_zscore"},
    {"table_name": "Crude_Net_Migration_Rate",       "id_col": "data_id", "value_col": "migration_rate",       "zscore_col": "migration_rate_zscore"},
    {"table_name": "Sex_Ratio_At_Birth",             "id_col": "data_id", "value_col": "sex_ratio_at_birth",   "zscore_col": "sex_ratio_birth_zscore"},
    {"table_name": "Sex_Ratio_Total_Population",     "id_col": "data_id", "value_col": "sex_ratio",            "zscore_col": "sex_ratio_total_zscore"},
    {"table_name": "Median_Age",                     "id_col": "data_id", "value_col": "age",                  "zscore_col": "median_age_zscore"},
    # -------------- sex‑specific tables --------------
    {"table_name": "life_expectancy_at_birth_by_sex", "id_col": "id",     "value_col": "life_expectancy",      "zscore_col": "life_expectancy_zscore"},
    {"table_name": "Infant_Mortality_Rate_By_Sex",    "id_col": "id",     "value_col": "infant_mortality_rate","zscore_col": "infant_mortality_zscore"},
    {"table_name": "Under_Five_Mortality_Rate_By_Sex","id_col": "id",     "value_col": "mortality_rate",       "zscore_col": "under_five_mortality_zscore"},
    {"table_name": "Population_by_sex",              "id_col": "id",     "value_col": "population",           "zscore_col": "population_by_sex_zscore"},
    {"table_name": "Population_By_Age_Group",        "id_col": "id",     "value_col": "population",           "zscore_col": "population_age_group_zscore"},
]

# ------------------------------------------------------------------
# Table-specific grouping configuration
# ------------------------------------------------------------------
# Dictionary to specify correct grouping columns for special tables
table_specific_grouping = {
    "Population_By_Age_Group": {
        "sex_cols": ["sex"],
        "age_cols": ["age_group_id"],  # Use age_group_id for grouping
    },
    "Population_by_sex": {
        "sex_cols": ["sex"],
    },
    "life_expectancy_at_birth_by_sex": {
        "sex_cols": ["sex"],
    },
    "Infant_Mortality_Rate_By_Sex": {
        "sex_cols": ["sex"],
    },
    "Under_Five_Mortality_Rate_By_Sex": {
        "sex_cols": ["sex"],
    }
}

# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------
connection = mysql.connector.connect(**db_config)
cursor     = connection.cursor()

def has_column(table, col):
    """True iff `table` has a column named `col` (case‑insensitive)."""
    cursor.execute(f"SHOW COLUMNS FROM `{table}` LIKE '{col}';")
    return cursor.fetchone() is not None

def get_all_columns(table):
    """Return list of all column names in the table."""
    cursor.execute(f"SHOW COLUMNS FROM `{table}`;")
    return [col[0] for col in cursor.fetchall()]

def ensure_zscore_column(table_name, zscore_col):
    """Add z‑score column if it doesn't exist."""
    if not has_column(table_name, zscore_col):
        cursor.execute(f"ALTER TABLE `{table_name}` "
                       f"ADD COLUMN `{zscore_col}` FLOAT NULL;")
        print(f"-> Added column `{zscore_col}` to {table_name}")
    else:
        print(f"-> Column `{zscore_col}` already exists in {table_name}")

# ------------------------------------------------------------------
# Core: rolling z‑score (sex‑aware and age‑aware)
# ------------------------------------------------------------------
def compute_and_update_zscores(table_name, id_col, value_col, zscore_col):
    # --- 1. Get all columns & detect special columns -----------------------
    all_columns = get_all_columns(table_name)
    year_exists = "year" in all_columns
    
    # --- 2. Determine sex and age columns ---------------------------------
    if table_name in table_specific_grouping:
        # Use predefined columns for special tables
        sex_cols = table_specific_grouping[table_name].get("sex_cols", [])
        age_cols = table_specific_grouping[table_name].get("age_cols", [])
        print(f"-> Using predefined grouping for {table_name}: sex={sex_cols}, age={age_cols}")
    else:
        # Default detection for standard tables
        sex_cols = [c for c in ["sex", "gender"] if c in all_columns]
        age_cols = [c for c in ["age_group", "age_range"] if c in all_columns]
    
    # --- 3. Build column lists for select and ordering ----------------------
    select_cols = [id_col, "country_id", "source_id"]
    order_cols = ["country_id", "source_id"]
    
    # Add detected columns to lists
    for col in sex_cols + age_cols:
        if col not in select_cols and col in all_columns:
            select_cols.append(col)
        if col not in order_cols and col in all_columns:
            order_cols.append(col)
    
    # Add year if it exists
    if year_exists:
        select_cols.append("year")
        order_cols.append("year")
    
    # Always include value column
    if value_col not in select_cols:
        select_cols.append(value_col)
    
    # --- 4. Fetch data ----------------------------------------------------
    query = f"SELECT {', '.join(select_cols)} FROM {table_name} WHERE {value_col} IS NOT NULL"
    print(f"-> Executing: {query}")
    cursor.execute(query)
    rows = cursor.fetchall()
    
    if not rows:
        print(f"No data found in {table_name}. Skipping.")
        return
    
    df = pd.DataFrame(rows, columns=select_cols)
    print(f"-> Fetched {len(df)} rows from {table_name}")
    
    # --- 5. Handle tables without year column -----------------------------
    if not year_exists:
        # Create a pseudo-time index for rolling calculations
        group_for_index = ["country_id", "source_id"] + sex_cols + age_cols
        df["row_idx"] = df.groupby(group_for_index).cumcount()
        order_cols.append("row_idx")
    
    df.sort_values(order_cols, inplace=True)
    
    # --- 6. Define grouping keys for rolling calculations ----------------
    # This is the key improvement - correct grouping by country, source, sex and age
    group_keys = ["country_id", "source_id"] + sex_cols + age_cols
    print(f"-> Using group keys for z-score: {group_keys}")
    
    # --- 7. Calculate rolling statistics by group ------------------------
    grp = df.groupby(group_keys, group_keys=False)
    
    # Calculate rolling mean and std within each group
    roll_mean = grp[value_col].apply(
        lambda s: s.rolling(window=WINDOW_YEARS, min_periods=MIN_PERIODS).mean()
    )
    roll_std = grp[value_col].apply(
        lambda s: s.rolling(window=WINDOW_YEARS, min_periods=MIN_PERIODS).std()
    )
    
    df["mean_val"] = roll_mean
    df["std_val"] = roll_std
    
    # --- 8. Calculate Z‑score --------------------------------------------
    def safe_z(r):
        if pd.isna(r["std_val"]) or r["std_val"] == 0:
            return None
        return (r[value_col] - r["mean_val"]) / r["std_val"]
    
    df[zscore_col] = df.apply(safe_z, axis=1)
    
    # Count non-null z-scores for reporting
    valid_zscores = df[zscore_col].count()
    print(f"-> Calculated {valid_zscores} valid z-scores")
    
    # --- 9. Update database ----------------------------------------------
    update_data = [
        (None if pd.isna(z) or np.isinf(z) else float(z), getattr(row, id_col))
        for z, row in zip(df[zscore_col], df.itertuples())
    ]
    
    cursor.executemany(
        f"UPDATE {table_name} SET {zscore_col} = %s WHERE {id_col} = %s",
        update_data,
    )
    connection.commit()
    print(f"-> Updated {table_name} with {len(update_data)} z-scores")

# ------------------------------------------------------------------
# Driver
# ------------------------------------------------------------------
def main():
    try:
        for meta in tables_info:
            print(f"\nProcessing table {meta['table_name']} …")
            ensure_zscore_column(meta["table_name"], meta["zscore_col"])
            compute_and_update_zscores(**meta)
        print("\nAll tables processed successfully.")
    except Exception as e:
        print(f"\nError while processing: {e}")
        import traceback
        traceback.print_exc()
        connection.rollback()
    finally:
        cursor.close()
        connection.close()

if __name__ == "__main__":
    main()