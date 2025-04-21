"""
update_source_credibility_score.py

This script computes and stores:

1. Per-source-per-country penalty scores in `Source_Country_Penalty`.
2. Overall source penalty scores (sum of penalties across all countries) in `Source_Overall_Credibility`.

Algorithm:
1. Fetch all (country_id, year) pairs from `Population_Anomaly_Explanations` where `confidence_level > 0`.
2. Build each source's coverage set from the `Population` table.
3. Initialize penalty_scores[(source_id, country_id)] = 0 for every source-country combination that appears in coverage.
4. For each anomaly pair:
     - Determine which sources flagged it via `population_anomalies`.
     - For each source covering that country but not flagged, subtract 1 penalty.
5. Upsert per-country penalties into `Source_Country_Penalty`.
6. Sum per-country penalties by source and upsert into `Source_Overall_Credibility`.
"""
import os
import mysql.connector
from mysql.connector import errorcode
from dotenv import load_dotenv

# Load environment variables
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

# ---------- Configuration ----------
db_config = {
    'user': 'root',
    'password': 'new_password',
    'host': '127.0.0.1',
    'database': 'fyp2',
    'raise_on_warnings': True
}

# Table names
TABLE_COUNTRY = 'Source_Country_Penalty'
TABLE_OVERALL = 'Source_Overall_Credibility'

# ---------- Schema Setup ----------
def create_country_table(cursor):
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_COUNTRY} (
            source_id    INT NOT NULL,
            country_id   INT NOT NULL,
            source_name  VARCHAR(255) NOT NULL,
            country_name VARCHAR(255) NOT NULL,
            penalty      FLOAT NOT NULL,
            PRIMARY KEY (source_id, country_id),
            FOREIGN KEY (source_id)  REFERENCES Data_Sources(source_id),
            FOREIGN KEY (country_id) REFERENCES Countries(country_id)
        );
    """)

def create_overall_table(cursor):
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_OVERALL} (
            source_id     INT NOT NULL,
            source_name   VARCHAR(255) NOT NULL,
            overall_score FLOAT NOT NULL,
            PRIMARY KEY (source_id),
            FOREIGN KEY (source_id) REFERENCES Data_Sources(source_id)
        );
    """)

# ---------- Computation ----------
def compute_penalties(cursor):
    # 1. high-confidence anomalies
    cursor.execute(
        "SELECT country_id, year FROM Population_Anomaly_Explanations WHERE confidence_level > 0;"
    )
    anomalies = cursor.fetchall()  # list of (cid, year)

    # 2. load sources and names
    cursor.execute("SELECT source_id, name FROM Data_Sources;")
    sources = cursor.fetchall()
    source_ids = [sid for sid, _ in sources]
    src_names  = {sid: nm for sid, nm in sources}

    # 3. build coverage map from Population
    coverage = {}
    for sid in source_ids:
        cursor.execute(
            "SELECT DISTINCT country_id FROM Population WHERE source_id = %s;",
            (sid,)
        )
        coverage[sid] = {r[0] for r in cursor.fetchall()}

    # 4. init penalties for each source-country in coverage
    penalties = {}
    # for all (sid, cid) pairs
    for sid in source_ids:
        for cid in coverage[sid]:
            penalties[(sid, cid)] = 0.0

    # 5. apply penalties
    for cid, year in anomalies:
        # flagged sources
        cursor.execute(
            "SELECT DISTINCT source_id FROM population_anomalies WHERE country_id = %s AND year = %s;",
            (cid, year)
        )
        flagged = {r[0] for r in cursor.fetchall()}
        for sid in source_ids:
            if cid in coverage[sid] and sid not in flagged:
                penalties[(sid, cid)] -= 1.0
    return penalties

# ---------- Upsert ----------
def upsert_country(cursor, penalties):
    # fetch country names
    country_names = {}
    cursor.execute("SELECT country_id, country_name FROM Countries;")
    for cid, cname in cursor.fetchall():
        country_names[cid] = cname

    for (sid, cid), pen in penalties.items():
        cursor.execute(f"SELECT name FROM Data_Sources WHERE source_id = %s;", (sid,))
        src_name = cursor.fetchone()[0]
        ctry_name = country_names.get(cid, '')
        cursor.execute(f"""
            INSERT INTO {TABLE_COUNTRY} (source_id, country_id, source_name, country_name, penalty)
            VALUES (%s, %s, %s, %s, %s) AS new
            ON DUPLICATE KEY UPDATE
              source_name  = new.source_name,
              country_name = new.country_name,
              penalty      = new.penalty;
        """, (sid, cid, src_name, ctry_name, pen))


def upsert_overall(cursor, penalties):
    # sum penalties per source
    sums = {}
    for (sid, _), pen in penalties.items():
        sums[sid] = sums.get(sid, 0.0) + pen
    for sid, total in sums.items():
        cursor.execute("SELECT name FROM Data_Sources WHERE source_id = %s;", (sid,))
        src_name = cursor.fetchone()[0]
        cursor.execute(f"""
            INSERT INTO {TABLE_OVERALL} (source_id, source_name, overall_score)
            VALUES (%s, %s, %s) AS new
            ON DUPLICATE KEY UPDATE
              source_name   = new.source_name,
              overall_score = new.overall_score;
        """, (sid, src_name, total))

# ---------- Main ----------
def main():
    cnx = mysql.connector.connect(**db_config)
    cursor = cnx.cursor()
    try:
        create_country_table(cursor)
        create_overall_table(cursor)
        cnx.commit()

        penalties = compute_penalties(cursor)
        upsert_country(cursor, penalties)
        upsert_overall(cursor, penalties)
        cnx.commit()
        print(f"Updated {TABLE_COUNTRY} and {TABLE_OVERALL} tables.")
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist.")
        else:
            print(f"MySQL error: {err}")
    finally:
        cursor.close()
        cnx.close()

if __name__ == '__main__':
    main()