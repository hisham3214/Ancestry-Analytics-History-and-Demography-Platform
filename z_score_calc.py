import mysql.connector
import pandas as pd
import numpy as np

WINDOW_YEARS = 5          # trailing window size
MIN_PERIODS  = 3          # how many points before we trust z-score

# ---------------------------------------------------------
# 1. Database Connection
# ---------------------------------------------------------
db_config = {
    'user': 'root',
    'password': 'LZ#amhe!32',
    'host': '127.0.0.1',
    'database': 'fyp2'
}

connection = mysql.connector.connect(**db_config)
cursor = connection.cursor()

# ---------------------------------------------------------
# 2. Define table metadata
# ---------------------------------------------------------
tables_info = [
    {
        "table_name": "Population",
        "id_col": "data_id",
        "value_col": "population",
        "zscore_col": "population_zscore"
    },
    {
        "table_name": "Birth_Rate",
        "id_col": "data_id",
        "value_col": "birth_rate",
        "zscore_col": "birth_rate_zscore"
    },
    {
        "table_name": "Death_Rate",
        "id_col": "data_id",
        "value_col": "death_rate",
        "zscore_col": "death_rate_zscore"
    },
    {
        "table_name": "Fertility_Rate",
        "id_col": "data_id",
        "value_col": "fertility_rate",
        "zscore_col": "fertility_rate_zscore"
    },
    {
        "table_name": "Total_Net_Migration",
        "id_col": "data_id",
        "value_col": "net_migration",
        "zscore_col": "net_migration_zscore"
    },
    {
        "table_name": "Crude_Net_Migration_Rate",
        "id_col": "data_id",
        "value_col": "migration_rate",
        "zscore_col": "migration_rate_zscore"
    },
    {
        "table_name": "Sex_Ratio_At_Birth",
        "id_col": "data_id",
        "value_col": "sex_ratio_at_birth",
        "zscore_col": "sex_ratio_birth_zscore"
    },
    {
        "table_name": "Sex_Ratio_Total_Population",
        "id_col": "data_id",
        "value_col": "sex_ratio",
        "zscore_col": "sex_ratio_total_zscore"
    },
    {
        "table_name": "Median_Age",
        "id_col": "data_id",
        "value_col": "age",
        "zscore_col": "median_age_zscore"
    },
    # sex-specific tables
    {
        "table_name": "life_expectancy_at_birth_by_sex",
        "id_col": "id",
        "value_col": "life_expectancy",
        "zscore_col": "life_expectancy_zscore"
    },
    {
        "table_name": "Infant_Mortality_Rate_By_Sex",
        "id_col": "id",
        "value_col": "infant_mortality_rate",
        "zscore_col": "infant_mortality_zscore"
    },
    {
        "table_name": "Under_Five_Mortality_Rate_By_Sex",
        "id_col": "id",
        "value_col": "mortality_rate",
        "zscore_col": "under_five_mortality_zscore"
    },
    {
        "table_name": "Population_by_sex",
        "id_col": "id",
        "value_col": "population",
        "zscore_col": "population_by_sex_zscore"
    },
    {
        "table_name": "Population_By_Age_Group",
        "id_col": "id",
        "value_col": "population",
        "zscore_col": "population_age_group_zscore"
    }
]

# ---------------------------------------------------------
# 3. Helper function: Ensure Z-score column exists
# ---------------------------------------------------------
def ensure_zscore_column(table_name, zscore_col):
    """
    Checks if a z-score column already exists in the table.
    If not, adds it as a FLOAT column (nullable).
    """
    cursor.execute(f"SHOW COLUMNS FROM `{table_name}` LIKE '{zscore_col}';")
    result = cursor.fetchone()

    if not result:
        alter_query = f"ALTER TABLE `{table_name}` ADD COLUMN `{zscore_col}` FLOAT NULL;"
        cursor.execute(alter_query)
        print(f"-> Added column `{zscore_col}` to {table_name}")
    else:
        print(f"-> Column `{zscore_col}` already exists in {table_name}")

# ---------------------------------------------------------
# 4. Sliding Window Z-score computation and update
# ---------------------------------------------------------
def compute_and_update_zscores(table_name, id_col, value_col, zscore_col):
    """
    Sliding‑window version:
      • sorts by year inside each (country_id, source_id) group
      • rolling mean/std over the last WINDOW_YEARS rows
      • writes NULL if fewer than MIN_PERIODS observations or σ == 0
    """
    fetch_query = f"""
        SELECT {id_col}, country_id, source_id, year, {value_col}
        FROM {table_name}
        WHERE {value_col} IS NOT NULL
    """
    cursor.execute(fetch_query)
    rows = cursor.fetchall()
    if not rows:
        print(f"No data found in {table_name}. Skipping.")
        return

    df = pd.DataFrame(rows,
                      columns=[id_col, "country_id", "source_id", "year", value_col])

    # -- 1. Order chronologically within each (country, source) ----
    df.sort_values(["country_id", "source_id", "year"], inplace=True)

    # -- 2. Rolling μ and σ ---------------------------------------
    grp = df.groupby(["country_id", "source_id"], group_keys=False)

    # If a table had no 'year', drop 'year' above and use .cumcount() as pseudo‑time.
    roll_mean = grp[value_col].apply(
        lambda s: s.rolling(window=WINDOW_YEARS, min_periods=MIN_PERIODS).mean())
    roll_std  = grp[value_col].apply(
        lambda s: s.rolling(window=WINDOW_YEARS, min_periods=MIN_PERIODS).std())

    df["mean_val"] = roll_mean
    df["std_val"]  = roll_std

    # -- 3. Z-score with safety guards ----------------------------
    def safe_z(row):
        if pd.isna(row["std_val"]) or row["std_val"] == 0:
            return None
        return (row[value_col] - row["mean_val"]) / row["std_val"]

    df[zscore_col] = df.apply(safe_z, axis=1)

    # -- 4. Prepare data for executemany --------------------------
    update_data = [
        (None if pd.isna(z) or np.isinf(z) else float(z), getattr(row, id_col))
        for z, row in zip(df[zscore_col], df.itertuples())
    ]

    update_query = f"""
        UPDATE {table_name}
        SET {zscore_col} = %s
        WHERE {id_col} = %s
    """
    cursor.executemany(update_query, update_data)
    connection.commit()
    print(f"-> Rolling z‑scores updated for {table_name}")
# ---------------------------------------------------------
# 5. Main script
# ---------------------------------------------------------
def main():
    try:
        for tbl in tables_info:
            table_name = tbl["table_name"]
            id_col = tbl["id_col"]
            value_col = tbl["value_col"]
            zscore_col = tbl["zscore_col"]

            print(f"Processing table {table_name}...")
            ensure_zscore_column(table_name, zscore_col)
            compute_and_update_zscores(table_name, id_col, value_col, zscore_col)

        print("All tables processed successfully.")

    except Exception as e:
        print("Error while processing:", e)
        connection.rollback()
    finally:
        cursor.close()
        connection.close()

if __name__ == "__main__":
    main()