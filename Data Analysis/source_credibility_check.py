import os
import mysql.connector
from mysql.connector import errorcode
from dotenv import load_dotenv

"""
update_source_credibility_score.py

This script computes and stores source credibility scores based on GPT-generated anomaly explanations.

Workflow:
1. Reads anomalies from `population_anomalies` table.
2. Joins each anomaly with its GPT-assigned confidence from `Population_Anomaly_Explanations`.
3. Calculates per-country credibility (average confidence per source-country).
4. Calculates overall credibility (average confidence per source across all anomalies).
5. Stores results in two tables: `Source_Credibility_Score` and `Source_Overall_Credibility`.

"""

# Load environment variables from .env
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

# ---------- Configuration ----------
# MySQL connection details
db_config = {
    'user': 'root',                   # MySQL username
    'password': 'new_password',  # load from .env
    'host': '127.0.0.1',
    'database': 'fyp2',               # your database
    'raise_on_warnings': True
}

# Table names
COUNTRY_SCORE_TABLE = 'Source_Credibility_Score'
OVERALL_SCORE_TABLE = 'Source_Overall_Credibility'

# ---------- Helper Functions ----------
def create_country_score_table(cursor):
    """
    Create the source credibility score table per country if it doesn't exist.
    Columns: source_id, country_id, country_name, score
    """
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {COUNTRY_SCORE_TABLE} (
        source_id     INT            NOT NULL,
        country_id    INT            NOT NULL,
        country_name  VARCHAR(255)   NOT NULL,
        score         FLOAT          NOT NULL,
        PRIMARY KEY (source_id, country_id),
        FOREIGN KEY (source_id)  REFERENCES Data_Sources(source_id),
        FOREIGN KEY (country_id) REFERENCES Countries(country_id)
    );
    """
    cursor.execute(create_sql)


def create_overall_score_table(cursor):
    """
    Create the overall source credibility score table if it doesn't exist.
    Columns: source_id, source_name, overall_score
    """
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {OVERALL_SCORE_TABLE} (
        source_id     INT            NOT NULL,
        source_name   VARCHAR(255)   NOT NULL,
        overall_score FLOAT          NOT NULL,
        PRIMARY KEY (source_id),
        FOREIGN KEY (source_id) REFERENCES Data_Sources(source_id)
    );
    """
    cursor.execute(create_sql)


def compute_country_scores(cursor):
    """
    Compute per-country credibility scores by averaging GPT confidences.
    """
    cursor.execute("SELECT DISTINCT source_id, country_id FROM population_anomalies;")
    pairs = cursor.fetchall()
    for source_id, country_id in pairs:
        cursor.execute(
            f"SELECT pae.confidence_level"
            f" FROM population_anomalies pa"
            f" JOIN Population_Anomaly_Explanations pae"
            f"   ON pa.country_id=pae.country_id AND pa.year=pae.year"
            f" WHERE pa.source_id=%s AND pa.country_id=%s;",
            (source_id, country_id)
        )
        confidences = [r[0] for r in cursor.fetchall()]
        score = float(sum(confidences)/len(confidences)) if confidences else 0.0
        cursor.execute("SELECT country_name FROM Countries WHERE country_id=%s", (country_id,))
        country_name = cursor.fetchone()[0]
        cursor.execute(
            f"INSERT INTO {COUNTRY_SCORE_TABLE} (source_id, country_id, country_name, score)"
            f" VALUES (%s, %s, %s, %s) AS new"
            f" ON DUPLICATE KEY UPDATE country_name=new.country_name, score=new.score;",
            (source_id, country_id, country_name, score)
        )


def compute_overall_scores(cursor):
    """
    Compute overall source credibility by averaging GPT confidences directly
    across all anomalies for each source.
    This weights each anomaly equally.
    """
    cursor.execute("SELECT DISTINCT source_id FROM population_anomalies;")
    sources = [row[0] for row in cursor.fetchall()]
    for source_id in sources:
        cursor.execute(
            f"SELECT pae.confidence_level"
            f" FROM population_anomalies pa"
            f" JOIN Population_Anomaly_Explanations pae"
            f"   ON pa.country_id=pae.country_id AND pa.year=pae.year"
            f" WHERE pa.source_id=%s;",
            (source_id,)
        )
        confidences = [r[0] for r in cursor.fetchall()]
        overall_score = float(sum(confidences)/len(confidences)) if confidences else 0.0
        cursor.execute(
            "SELECT name FROM Data_Sources WHERE source_id=%s", (source_id,)
        )
        source_name = cursor.fetchone()[0]
        cursor.execute(
            f"INSERT INTO {OVERALL_SCORE_TABLE} (source_id, source_name, overall_score)"
            f" VALUES (%s, %s, %s) AS new"
            f" ON DUPLICATE KEY UPDATE source_name=new.source_name, overall_score=new.overall_score;",
            (source_id, source_name, overall_score)
        )


def main():
    cnx = mysql.connector.connect(**db_config)
    cursor = cnx.cursor()
    try:
        create_country_score_table(cursor)
        create_overall_score_table(cursor)
        cnx.commit()
        compute_country_scores(cursor)
        compute_overall_scores(cursor)
        cnx.commit()
        print(f"Scores updated in {COUNTRY_SCORE_TABLE} and {OVERALL_SCORE_TABLE}.")
    except mysql.connector.Error as err:
        print(f"MySQL error: {err}")
    finally:
        cursor.close()
        cnx.close()

if __name__ == "__main__":
    main()
