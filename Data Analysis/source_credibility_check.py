#!/usr/bin/env python3
"""
update_source_credibility_score.py

This script computes and stores:

1. Per-source-per-country penalty scores in `Source_Country_Penalty`.
2. Softmax-normalized weights in the `normalized_weight` column.
"""

import os
import mysql.connector
from mysql.connector import errorcode
from dotenv import load_dotenv
import math
from collections import defaultdict

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

# Table name
TABLE_COUNTRY = 'Source_Country_Penalty'

def create_country_table(cursor):
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_COUNTRY} (
            source_id         INT NOT NULL,
            country_id        INT NOT NULL,
            source_name       VARCHAR(255) NOT NULL,
            country_name      VARCHAR(255) NOT NULL,
            penalty           FLOAT NOT NULL,
            normalized_weight DOUBLE NOT NULL DEFAULT 0,
            PRIMARY KEY (source_id, country_id),
            FOREIGN KEY (source_id)  REFERENCES Data_Sources(source_id),
            FOREIGN KEY (country_id) REFERENCES Countries(country_id)
        );
    """)

def compute_penalties(cursor):
    # 1. Fetch high-confidence anomalies
    cursor.execute(
        "SELECT country_id, year FROM Population_Anomaly_Explanations WHERE confidence_level > 0;"
    )
    anomalies = cursor.fetchall()

    # 2. Load sources
    cursor.execute("SELECT source_id, name FROM Data_Sources;")
    sources = cursor.fetchall()
    source_ids = [sid for sid, _ in sources]

    # 3. Build coverage map
    coverage = {}
    for sid in source_ids:
        cursor.execute(
            "SELECT DISTINCT country_id FROM Population WHERE source_id = %s;", (sid,)
        )
        coverage[sid] = {r[0] for r in cursor.fetchall()}

    # 4. Initialize penalties
    penalties = {(sid, cid): 0.0
                 for sid in source_ids
                 for cid in coverage.get(sid, [])}

    # 5. Apply penalty for missing anomalies
    for cid, year in anomalies:
        cursor.execute(
            "SELECT DISTINCT source_id FROM population_anomalies WHERE country_id = %s AND year = %s;",
            (cid, year)
        )
        flagged = {r[0] for r in cursor.fetchall()}
        for sid in source_ids:
            if cid in coverage.get(sid, []) and sid not in flagged:
                penalties[(sid, cid)] -= 1.0

    return penalties

def upsert_country(cursor, penalties):
    cursor.execute("SELECT country_id, country_name FROM Countries;")
    country_names = {cid: cname for cid, cname in cursor.fetchall()}

    for (sid, cid), pen in penalties.items():
        cursor.execute("SELECT name FROM Data_Sources WHERE source_id = %s;", (sid,))
        src_name = cursor.fetchone()[0]
        ctry_name = country_names.get(cid, '')
        cursor.execute(f"""
            INSERT INTO {TABLE_COUNTRY} (source_id, country_id, source_name, country_name, penalty)
            VALUES (%s, %s, %s, %s, %s) AS new
            ON DUPLICATE KEY UPDATE
              source_name       = new.source_name,
              country_name      = new.country_name,
              penalty           = new.penalty;
        """, (sid, cid, src_name, ctry_name, pen))

def normalize_country_weights(cursor):
    cursor.execute(f"SELECT country_id, source_id, penalty FROM {TABLE_COUNTRY};")
    rows = cursor.fetchall()
    by_country = defaultdict(list)
    for cid, sid, pen in rows:
        by_country[cid].append((sid, pen))

    for cid, items in by_country.items():
        creds = [ -pen for (_, pen) in items ]
        m = max(creds)
        exps = [ math.exp(c - m) for c in creds ]
        total = sum(exps)
        for (sid, _), e in zip(items, exps):
            w = e / total
            cursor.execute(f"""
                UPDATE {TABLE_COUNTRY}
                   SET normalized_weight = %s
                 WHERE country_id = %s AND source_id = %s;
            """, (w, cid, sid))

def main():
    cnx = mysql.connector.connect(**db_config)
    cursor = cnx.cursor()
    try:
        create_country_table(cursor)
        cnx.commit()

        penalties = compute_penalties(cursor)
        upsert_country(cursor, penalties)
        cnx.commit()

        normalize_country_weights(cursor)
        cnx.commit()

        print(f"Updated {TABLE_COUNTRY} (penalties & weights).")

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