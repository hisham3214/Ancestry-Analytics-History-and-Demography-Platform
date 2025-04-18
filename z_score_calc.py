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
# Helpers
# ------------------------------------------------------------------
connection = mysql.connector.connect(**db_config)
cursor     = connection.cursor()

def has_column(table, col):
    """True iff `table` has a column named `col` (case‑insensitive)."""
    cursor.execute(f"SHOW COLUMNS FROM `{table}` LIKE '{col}';")
    return cursor.fetchone() is not None

def ensure_zscore_column(table_name, zscore_col):
    """Add z‑score column if it doesn't exist."""
    if not has_column(table_name, zscore_col):
        cursor.execute(f"ALTER TABLE `{table_name}` "
                       f"ADD COLUMN `{zscore_col}` FLOAT NULL;")
        print(f"-> Added column `{zscore_col}` to {table_name}")
    else:
        print(f"-> Column `{zscore_col}` already exists in {table_name}")

# ------------------------------------------------------------------
# Core: rolling z‑score (sex‑aware)
# ------------------------------------------------------------------
def compute_and_update_zscores(table_name, id_col, value_col, zscore_col):
    # ----------------------------------------------------------------
    # 1) figure out what columns we can SELECT
    # ----------------------------------------------------------------
    year_exists = has_column(table_name, "year")
    sex_exists  = has_column(table_name, "sex") or has_column(table_name, "gender")
    sex_col     = "sex" if has_column(table_name, "sex") else ("gender" if has_column(table_name, "gender") else None)

    select_cols  = [id_col, "country_id", "source_id"]
    order_cols   = ["country_id", "source_id"]
    if year_exists:
        select_cols.append("year")
        order_cols.append("year")
    if sex_col:
        select_cols.append(sex_col)
        order_cols.insert(2, sex_col)          # order … country, source, sex, year

    select_cols.append(value_col)

    # ----------------------------------------------------------------
    # 2) fetch data
    # ----------------------------------------------------------------
    fetch_query = f"""
        SELECT {', '.join(select_cols)}
        FROM {table_name}
        WHERE {value_col} IS NOT NULL
    """
    cursor.execute(fetch_query)
    rows = cursor.fetchall()
    if not rows:
        print(f"No data found in {table_name}. Skipping.")
        return

    df = pd.DataFrame(rows, columns=select_cols)

    # ----------------------------------------------------------------
    # 3) add pseudo‑time if year absent
    # ----------------------------------------------------------------
    if not year_exists:
        df["row_idx"] = df.groupby(["country_id", "source_id"] + ([sex_col] if sex_col else [])).cumcount()
        order_cols.append("row_idx")   # ensures chronological order

    # chronological order inside group(s)
    df.sort_values(order_cols, inplace=True)

    # ----------------------------------------------------------------
    # 4) build rolling baselines
    # ----------------------------------------------------------------
    group_keys = ["country_id", "source_id"]
    if sex_col:
        group_keys.append(sex_col)

    grp        = df.groupby(group_keys, group_keys=False)
    roll_mean  = grp[value_col].apply(lambda s: s.rolling(window=WINDOW_YEARS,
                                                          min_periods=MIN_PERIODS).mean())
    roll_std   = grp[value_col].apply(lambda s: s.rolling(window=WINDOW_YEARS,
                                                          min_periods=MIN_PERIODS).std())

    df["mean_val"] = roll_mean
    df["std_val"]  = roll_std

    # ----------------------------------------------------------------
    # 5) z‑score with safety guards
    # ----------------------------------------------------------------
    def safe_z(row):
        if pd.isna(row["std_val"]) or row["std_val"] == 0:
            return None
        return (row[value_col] - row["mean_val"]) / row["std_val"]

    df[zscore_col] = df.apply(safe_z, axis=1)

    # ----------------------------------------------------------------
    # 6) push back to MySQL
    # ----------------------------------------------------------------
    update_data = [
        (None if pd.isna(z) or np.isinf(z) else float(z), getattr(r, id_col))
        for z, r in zip(df[zscore_col], df.itertuples())
    ]

    cursor.executemany(
        f"UPDATE {table_name} SET {zscore_col} = %s WHERE {id_col} = %s",
        update_data,
    )
    connection.commit()
    print(f"-> Rolling z‑scores updated for {table_name}")

# ------------------------------------------------------------------
# Driver
# ------------------------------------------------------------------
def main():
    try:
        for meta in tables_info:
            print(f"Processing table {meta['table_name']} …")
            ensure_zscore_column(meta["table_name"], meta["zscore_col"])
            compute_and_update_zscores(**meta)
        print("All tables processed successfully.")
    except Exception as e:
        print("Error while processing:", e)
        connection.rollback()
    finally:
        cursor.close()
        connection.close()

if __name__ == "__main__":
    main()
