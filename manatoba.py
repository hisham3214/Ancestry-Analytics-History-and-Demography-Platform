import uuid
import datetime
import logging

import mysql.connector
import pandas as pd
import numpy as np
from mysql.connector import Error as MySQLError

from sklearn.covariance import MinCovDet
from scipy.stats import chi2

# ─── CONFIG ────────────────────────────────────────────────────────────────────

DB_CONFIG = {
    'user': 'root',
    'password': 'LZ#amhe!32',
    'host': '127.0.0.1',
    'database': 'fyp2',
    'raise_on_warnings': True,
}

ANOMALY_TABLE = 'Country_Year_Anomalies'
ROLLING_WINDOW = 5
MIN_PERIODS = 3
ALPHA = 0.003  # χ² tail threshold for flag
EPSILON = 1e-10  # small epsilon to prevent div-by-zero
MCD_SUPPORT_FRACTION = 0.8  # default support fraction for MinCovDet

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─── HELPERS ───────────────────────────────────────────────────────────────────

def rolling_mad(x):
    med = np.median(x)
    return np.median(np.abs(x - med))

# ─── STEP 1: LOAD & MERGE INDICATORS ───────────────────────────────────────────

def load_indicators(conn):
    """
    Pulls multi-source indicators from the DB, computing avg/max/min and source counts.
    Falls back with empty DataFrame shapes on load errors.
    """
    queries = {
        'pop': (
            """
            SELECT country_id, year,
                   AVG(population)     AS avg_population,
                   MAX(population)     AS max_population,
                   MIN(population)     AS min_population,
                   COUNT(DISTINCT source_id) AS n_src_population
            FROM Population
            GROUP BY country_id, year
            """
        ),
        'birth': (
            """
            SELECT country_id, year,
                   AVG(birth_rate) AS avg_birth_rate,
                   MAX(birth_rate) AS max_birth_rate,
                   MIN(birth_rate) AS min_birth_rate,
                   COUNT(DISTINCT source_id) AS n_src_birth_rate
            FROM Birth_Rate
            GROUP BY country_id, year
            """
        ),
        'death': (
            """
            SELECT country_id, year,
                   AVG(death_rate) AS avg_death_rate,
                   MAX(death_rate) AS max_death_rate,
                   MIN(death_rate) AS min_death_rate,
                   COUNT(DISTINCT source_id) AS n_src_death_rate
            FROM Death_Rate
            GROUP BY country_id, year
            """
        ),
        'fert': (
            """
            SELECT country_id, year,
                   AVG(Fertility_rate) AS avg_fertility_rate,
                   MAX(Fertility_rate) AS max_fertility_rate,
                   MIN(Fertility_rate) AS min_fertility_rate,
                   COUNT(DISTINCT source_id) AS n_src_fertility_rate
            FROM Fertility_Rate
            GROUP BY country_id, year
            """
        ),
        'mig': (
            """
            SELECT country_id, year,
                   AVG(net_migration) AS avg_net_migration,
                   MAX(net_migration) AS max_net_migration,
                   MIN(net_migration) AS min_net_migration,
                   COUNT(DISTINCT source_id) AS n_src_net_migration
            FROM Total_Net_Migration
            GROUP BY country_id, year
            """
        ),
        'age': (
            """
            SELECT country_id, year,
                   AVG(age) AS avg_median_age
            FROM Median_Age
            GROUP BY country_id, year
            """
        ),
        'life': (
            """
            SELECT country_id, year,
                   AVG(life_expectancy) AS avg_life_exp,
                   MAX(CASE WHEN sex='Male'   THEN life_expectancy END)
                     - MAX(CASE WHEN sex='Female' THEN life_expectancy END)
                     AS sex_gap_life_exp
            FROM life_expectancy_at_birth_by_sex
            GROUP BY country_id, year
            """
        ),
    }

    # expected columns for fallback DataFrames
    expected_cols = {
        'pop':   ['country_id','year','avg_population','max_population','min_population','n_src_population'],
        'birth': ['country_id','year','avg_birth_rate','max_birth_rate','min_birth_rate','n_src_birth_rate'],
        'death': ['country_id','year','avg_death_rate','max_death_rate','min_death_rate','n_src_death_rate'],
        'fert':  ['country_id','year','avg_fertility_rate','max_fertility_rate','min_fertility_rate','n_src_fertility_rate'],
        'mig':   ['country_id','year','avg_net_migration','max_net_migration','min_net_migration','n_src_net_migration'],
        'age':   ['country_id','year','avg_median_age'],
        'life':  ['country_id','year','avg_life_exp','sex_gap_life_exp'],
    }

    dfs = []
    for key, sql in queries.items():
        if key not in expected_cols:
            raise KeyError(f"No expected_cols defined for '{key}'")
        try:
            df = pd.read_sql(sql, conn)
            logger.info(f"Loaded '{key}' with {len(df)} rows")
        except Exception as e:
            logger.error(f"Failed to load '{key}': {e}")
            df = pd.DataFrame(columns=expected_cols[key])
        dfs.append(df)

    if not dfs:
        raise ValueError("No data loaded for any indicator")

    # merge all on country_id & year
    from functools import reduce
    merged = reduce(
        lambda left, right: pd.merge(left, right, on=['country_id','year'], how='outer'),
        dfs
    )
    return merged

# ─── STEP 2: FEATURE ENGINEERING ───────────────────────────────────────────────

def engineer(df):
    df = df.sort_values(['country_id','year']).reset_index(drop=True)

    # safe year scaling
    y_min, y_max = df['year'].min(), df['year'].max()
    span = (y_max - y_min) or EPSILON
    df['year_scaled'] = -1 + 2 * (df['year'] - y_min) / span

    # robust z-scores via median & MAD
    for col in [
        'avg_population', 'avg_birth_rate', 'avg_death_rate',
        'avg_fertility_rate','avg_net_migration','avg_median_age','avg_life_exp'
    ]:
        grp = df.groupby('country_id')[col]
        med = grp.rolling(ROLLING_WINDOW, min_periods=MIN_PERIODS, center=True)\
                  .median().reset_index(level=0, drop=True)
        mad = grp.rolling(ROLLING_WINDOW, min_periods=MIN_PERIODS, center=True)\
                  .apply(rolling_mad, raw=True).reset_index(level=0, drop=True)
        mad = mad.replace(0, EPSILON)
        df[f'z_{col}'] = (df[col] - med) / (1.4826 * mad)

    # absolute diffs
    df['delta_population']     = df.groupby('country_id')['avg_population'].diff()
    df['delta_birth_rate']     = df.groupby('country_id')['avg_birth_rate'].diff()
    df['natural_increase_rate']= df['avg_birth_rate'] - df['avg_death_rate']

    # range ratios
    df['range_ratio_population']   = (df['max_population']   - df['min_population'])   / (df['avg_population']   + EPSILON)
    df['range_ratio_birth_rate']   = (df['max_birth_rate']   - df['min_birth_rate'])   / (df['avg_birth_rate']   + EPSILON)
    df['range_ratio_death_rate']   = (df['max_death_rate']   - df['min_death_rate'])   / (df['avg_death_rate']   + EPSILON)
    df['range_ratio_fertility']    = (df['max_fertility_rate']- df['min_fertility_rate'])/ (df['avg_fertility_rate']+EPSILON)
    df['range_ratio_net_mig']      = (df['max_net_migration'] - df['min_net_migration']) / (df['avg_net_migration'] +EPSILON)

    # rename source counts
    df['n_sources_population']   = df['n_src_population']
    df['n_sources_birth_rate']   = df['n_src_birth_rate']
    df['n_sources_death_rate']   = df['n_src_death_rate']
    df['n_sources_fertility']    = df['n_src_fertility_rate']
    df['n_sources_net_migration']= df['n_src_net_migration']

    # drop internal columns
    df = df.drop(columns=[c for c in df if c.startswith('n_src_')])

    # assemble feature list and drop NaNs
    feature_prefixes = ('z_','delta_','natural_increase_rate','range_ratio_','year_scaled','n_sources','sex_gap_life_exp')
    features = [c for c in df.columns if c.startswith(feature_prefixes)]
    before = len(df)
    df_clean = df.dropna(subset=features)
    dropped = before - len(df_clean)
    logger.info(f"Dropped {dropped} rows with NaN features")

    return df_clean, features

# ─── STEP 3: MAHALANOBIS & P‑VALUE ─────────────────────────────────────────────

def compute_md(df, features):
    X = df[features].values
    n_feats = len(features)
    min_req = 5 * n_feats

    if len(X) < min_req:
        support_fraction = max(0.5, len(X) / (2 * n_feats))
        logger.info(f"Adjusted support_fraction to {support_fraction:.2f} for {len(X)} rows and {n_feats} features")
    else:
        support_fraction = MCD_SUPPORT_FRACTION

    mcd = MinCovDet(support_fraction=support_fraction).fit(X)
    md_vals = mcd.mahalanobis(X)
    pvals   = chi2.sf(md_vals, df=n_feats)
    flags   = (pvals < ALPHA).astype(int)

    df['mahalanobis'] = md_vals
    df['md_pvalue']   = pvals
    df['md_flag']     = flags
    return df

# ─── STEP 4: WRITE RESULTS ────────────────────────────────────────────────────

def write_results(conn, df):
    run_id = str(uuid.uuid4())
    now    = datetime.datetime.utcnow()
    cursor = conn.cursor()
    try:
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {ANOMALY_TABLE} (
          country_id   INT,
          year         INT,
          mahalanobis  FLOAT,
          md_flag      TINYINT,
          md_pvalue    FLOAT,
          run_id       VARCHAR(36),
          created_at   TIMESTAMP,
          PRIMARY KEY(country_id,year,run_id)
        )""")

        valid = df.dropna(subset=['mahalanobis'])
        if valid.empty:
            logger.warning("No valid rows to insert, skipping write")
            return

        sql = f"""
        INSERT INTO {ANOMALY_TABLE}
          (country_id,year,mahalanobis,md_flag,md_pvalue,run_id,created_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s)"""
        data = [(
            int(r.country_id), int(r.year),
            float(r.mahalanobis), int(r.md_flag),
            float(r.md_pvalue), run_id, now
        ) for _, r in valid.iterrows()]

        cursor.executemany(sql, data)
        conn.commit()
        logger.info(f"Inserted {len(data)} anomaly rows (run_id={run_id})")

    except MySQLError as err:
        conn.rollback()
        logger.error(f"MySQL error during write: {err}")
        raise
    finally:
        cursor.close()

# ─── MAIN & ENTRY POINT ───────────────────────────────────────────────────────

def main():
    conn = mysql.connector.connect(**DB_CONFIG)
    raw, feat_cols = None, None
    try:
        raw = load_indicators(conn)
        feat_df, feat_cols = engineer(raw)
        anom_df = compute_md(feat_df, feat_cols)
        write_results(conn, anom_df)
    finally:
        if conn.is_connected():
            conn.close()
            logger.info("Database connection closed")

if __name__ == '__main__':
    import sys
    try:
        main()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
