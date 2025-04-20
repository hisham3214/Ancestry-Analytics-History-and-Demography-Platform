import os
import json
import time
import mysql.connector
from mysql.connector import errorcode
import openai
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# ---------- Configuration ----------
# MySQL connection details
db_config = {
    'user': 'root',
    'password': 'new_password',  # replace with your MySQL password
    'host': '127.0.0.1',
    'database': 'fyp2',
    'raise_on_warnings': True
}
# OpenAI API key from environment
openai.api_key = os.getenv('OPENAI_API_KEY')

# Table for storing GPT explanations
EXPL_TABLE = 'Population_Anomaly_Explanations'

# ---------- Helper Functions ----------
def create_explanations_table(cursor):
    """
    Create the explanations table if it doesn't exist.
    Columns: country_id, country_name, year, confidence_level, explanation
    """
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {EXPL_TABLE} (
        country_id       INT            NOT NULL,
        country_name     VARCHAR(255)   NOT NULL,
        year             INT            NOT NULL,
        confidence_level INT            NOT NULL,
        explanation      TEXT           NOT NULL,
        PRIMARY KEY (country_id, year),
        FOREIGN KEY (country_id) REFERENCES Countries(country_id)
    );
    """
    cursor.execute(create_sql)


def fetch_anomalies(cursor):
    """
    Retrieve country_id, country_name, and year for each anomaly.
    """
    cursor.execute(
        f"""
        SELECT a.country_id,
               c.country_name,
               a.year
        FROM population_anomalies a
        JOIN Countries c USING(country_id);
        """
    )
    return cursor.fetchall()


def ask_gpt_for_reason(country_name, year, max_retries=3):
    """
    Query GPT for the cause of the demographic anomaly in a given country and year.
    Returns (reason: str, confidence: int) or (None, None) on failure.
    """
    prompt = (
        f"You are an expert historian and demographer.\n"
        f"A demographic anomaly was detected in {country_name} in {year}—its population deviated significantly from expected trends.\n"
        f"Identify any event in {year} (or up to two years prior) that directly caused this change (e.g. war, disaster, mass migration).\n"
        f"If you believe this anomaly is false (i.e., no significant event occurred), say so explicitly.\n"
        f"Respond only with a JSON object containing exactly two fields:\n"
        f"  \"reason\": a single sentence (≤20 words) naming the event and its direct impact or stating 'No known event' if false.\n"
        f"  \"confidence\": an integer 1–5 for how sure you are this event caused the anomaly or for 'No known event'.\n"
    )

    for attempt in range(1, max_retries + 1):
        try:
            response = openai.ChatCompletion.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
                max_tokens=150
            )
            content = response.choices[0].message.content.strip()
            # Extract JSON part
            start = content.find('{')
            end = content.rfind('}')
            json_str = content[start:end+1] if start != -1 and end != -1 else content
            data = json.loads(json_str)
            reason = data.get('reason', '').strip()
            confidence = int(data.get('confidence', 0))
            return reason, confidence

        except json.JSONDecodeError:
            print(f"Attempt {attempt}: Failed to parse JSON: {repr(content)}")
        except Exception as e:
            print(f"Attempt {attempt}: GPT API error for {country_name},{year}: {e}")

        time.sleep(1)
    return None, None


def insert_explanation(cursor, country_id, country_name, year, confidence, explanation):
    """
    Insert or update the GPT explanation record.
    """
    insert_sql = f"""
    INSERT INTO {EXPL_TABLE} (country_id, country_name, year, confidence_level, explanation)
    VALUES (%s, %s, %s, %s, %s) AS new
    ON DUPLICATE KEY UPDATE
      country_name     = new.country_name,
      confidence_level = new.confidence_level,
      explanation      = new.explanation;
    """
    cursor.execute(insert_sql, (country_id, country_name, year, confidence, explanation))


# ---------- Main Workflow ----------
if __name__ == "__main__":
    cnx = mysql.connector.connect(**db_config)
    cursor = cnx.cursor()
    try:
        create_explanations_table(cursor)
        cnx.commit()

        anomalies = fetch_anomalies(cursor)
        print(f"Found {len(anomalies)} anomalies to explain.")

        for country_id, country_name, year in anomalies:
            print(f"Processing anomaly: {country_name} ({country_id}), year={year}")
            reason, confidence = ask_gpt_for_reason(country_name, year)
            if reason and confidence:
                insert_explanation(cursor, country_id, country_name, year, confidence, reason)
                cnx.commit()
                print(f"  -> Saved: {country_name}, {reason} (confidence {confidence})")
            else:
                print(f"  -> Skipped {country_name},{year} after retries.")

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist.")
        else:
            print(err)
    finally:
        cursor.close()
        cnx.close()
