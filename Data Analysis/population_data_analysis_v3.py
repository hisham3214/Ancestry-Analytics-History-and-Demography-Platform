import mysql.connector
import pandas as pd
import numpy as np
from scipy import stats
import logging
import os
from datetime import datetime
from mysql.connector import Error

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PopulationDataAnalyzer:
    def __init__(self, db_config, results_dir="analysis_results"):
        self.db_config = db_config
        self.connection = None
        self.results_dir = results_dir
        try:
            if not os.path.exists(results_dir):
                os.makedirs(results_dir)
            self.plots_dir = os.path.join(results_dir, "plots")
            self.data_dir = os.path.join(results_dir, "data")
            for d in (self.plots_dir, self.data_dir):
                if not os.path.exists(d):
                    os.makedirs(d)
        except OSError as e:
            logger.error(f"Failed to create directory structure: {str(e)}")
            raise

    def connect_to_database(self):
        try:
            if not self.connection or not self.connection.is_connected():
                self.connection = mysql.connector.connect(**self.db_config)
                logger.info("Database connection established successfully")
            return True
        except Error as e:
            logger.error(f"Error connecting to MySQL database: {str(e)}")
            return False

    def close_connection(self):
        try:
            if self.connection and self.connection.is_connected():
                self.connection.close()
                logger.info("Database connection closed successfully")
        except Error as e:
            logger.error(f"Error closing database connection: {str(e)}")

    def execute_query(self, query, params=None, fetch=True, commit=False):
        cursor = None
        results = None
        try:
            if not self.connect_to_database():
                return None
            cursor = self.connection.cursor(dictionary=True)
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            if fetch:
                results = cursor.fetchall()
            if commit:
                self.connection.commit()
            return results
        except Error as e:
            logger.error(f"Database query error: {str(e)}")
            if self.connection and self.connection.is_connected():
                self.connection.rollback()
            return None
        finally:
            if cursor:
                cursor.close()

    def extract_population_data(self, start_year=1950, end_year=2025):
        query = f"""
        SELECT p.country_id, p.source_id, p.year, p.population, c.country_name, s.name AS source_name
        FROM Population p
        JOIN Countries c ON p.country_id = c.country_id
        JOIN Data_Sources s ON p.source_id = s.source_id
        WHERE p.year BETWEEN %s AND %s
        ORDER BY p.country_id, p.year
        """
        try:
            results = self.execute_query(query, (start_year, end_year))
            if results is not None:
                df = pd.DataFrame(results)
                logger.info(f"Successfully extracted {len(df)} population records")
                return df
            else:
                logger.warning("No population data retrieved")
                return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error in extract_population_data: {str(e)}")
            return pd.DataFrame()

    def detect_anomalies_z_score(self, df, threshold=3.0):
        try:
            result = []
            for (_, group) in df.groupby(['country_id', 'source_id']):
                g = group.sort_values('year').copy()
                if len(g) > 1:
                    g['population_z'] = stats.zscore(g['population'])
                    g['is_z_anomaly'] = g['population_z'].abs() > threshold
                else:
                    g['population_z'] = 0.0
                    g['is_z_anomaly'] = False
                result.append(g)
            return pd.concat(result) if result else pd.DataFrame()
        except Exception as e:
            logger.error(f"Error in Z-score anomaly detection: {str(e)}")
            return pd.DataFrame()

    def detect_anomalies_yoy_advanced(self, df, z_threshold=2.0, window_size=5, second_deriv_threshold=0.03):
        try:
            result = []
            for (_, group) in df.groupby(['country_id', 'source_id']):
                g = group.sort_values('year').copy()
                if len(g) < max(3, window_size):
                    continue
                g['prev'] = g['population'].shift(1)
                g['yoy_change'] = ((g['population'] - g['prev']) / g['prev']).replace([np.inf, -np.inf], np.nan).fillna(0)
                mean = g['yoy_change'].mean(); std = g['yoy_change'].std()
                if std > 0:
                    g['global_z_score'] = (g['yoy_change'] - mean) / std
                    g['is_global_yoy_anomaly'] = g['global_z_score'].abs() > z_threshold
                else:
                    g['global_z_score'] = 0.0
                    g['is_global_yoy_anomaly'] = False
                if len(g) >= window_size:
                    roll = g['yoy_change'].rolling(window_size, min_periods=2)
                    g['rolling_z'] = (g['yoy_change'] - roll.mean()) / roll.std()
                    g['is_rolling_yoy_anomaly'] = g['rolling_z'].abs() > z_threshold
                else:
                    g['rolling_z'] = np.nan
                    g['is_rolling_yoy_anomaly'] = False
                g['prev_yoy'] = g['yoy_change'].shift(1)
                g['second_derivative'] = (g['yoy_change'] - g['prev_yoy']).fillna(0)
                g['is_acceleration_anomaly'] = g['second_derivative'].abs() > second_deriv_threshold
                g['is_yoy_anomaly'] = g['is_global_yoy_anomaly'] | g['is_rolling_yoy_anomaly'] | g['is_acceleration_anomaly']
                g['is_increase_anomaly'] = g['is_yoy_anomaly'] & (g['yoy_change'] > 0)
                g['is_decrease_anomaly'] = g['is_yoy_anomaly'] & (g['yoy_change'] < 0)
                result.append(g)
            return pd.concat(result) if result else pd.DataFrame()
        except Exception as e:
            logger.error(f"Error in YoY anomaly detection: {str(e)}")
            return pd.DataFrame()

    def analyze_population_data(self, start_year=1950, end_year=2025):
        try:
            pop_df = self.extract_population_data(start_year, end_year)
            if pop_df.empty:
                logger.warning("No population data available for analysis")
                return {'status': 'warning', 'message': 'No data available', 'anomalies': 0}
            z_df = self.detect_anomalies_z_score(pop_df)
            yoy_df = self.detect_anomalies_yoy_advanced(pop_df)
            if not z_df.empty and not yoy_df.empty:
                df = yoy_df.merge(
                    z_df[['country_id', 'source_id', 'year', 'population_z', 'is_z_anomaly']],
                    on=['country_id', 'source_id', 'year'], how='outer'
                )
                is_z = df['is_z_anomaly'].fillna(False)
                is_yoy = df.get('is_yoy_anomaly', pd.Series(False, index=df.index)).fillna(False)
                df['is_any_anomaly'] = is_z | is_yoy
            else:
                df = z_df if not z_df.empty else yoy_df
                if 'is_z_anomaly' not in df.columns:
                    df['is_z_anomaly'] = False
                if 'is_yoy_anomaly' not in df.columns:
                    df['is_yoy_anomaly'] = False
                df['is_any_anomaly'] = df['is_z_anomaly'] | df['is_yoy_anomaly']
            anomaly_count = 0
            if not df.empty:
                anomalies_df = df[df['is_any_anomaly']]
                anomaly_count = len(anomalies_df)
                if anomaly_count > 0 and not self.save_anomalies_to_db(anomalies_df):
                    return {'status': 'error', 'message': 'Failed to save anomalies', 'anomalies': anomaly_count}
            logger.info(f"Analysis completed. Found {anomaly_count} anomalies.")
            return {'status': 'success', 'anomalies': anomaly_count}
        except Exception as e:
            logger.error(f"Error analyzing population data: {str(e)}")
            return {'status': 'error', 'message': str(e), 'anomalies': 0}

    def save_anomalies_to_db(self, df):
        if df.empty:
            logger.info("No anomalies to save")
            return True
        try:
            insert = """
            INSERT INTO Population_Anomalies (
                country_id, source_id, year, population_z, yoy_change,
                global_z_score, rolling_z, second_derivative,
                is_z_anomaly, is_global_yoy_anomaly, is_rolling_yoy_anomaly,
                is_acceleration_anomaly, is_yoy_anomaly, is_increase_anomaly,
                is_decrease_anomaly
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            records = []
            for _, r in df.iterrows():
                population_z = None if pd.isna(r.get('population_z')) else float(r['population_z'])
                yoy_change = None if pd.isna(r.get('yoy_change')) else float(r['yoy_change'])
                global_z = None if pd.isna(r.get('global_z_score')) else float(r['global_z_score'])
                rolling_z = None if pd.isna(r.get('rolling_z')) else float(r['rolling_z'])
                second_deriv = None if pd.isna(r.get('second_derivative')) else float(r['second_derivative'])
                records.append((
                    int(r['country_id']), int(r['source_id']), int(r['year']),
                    population_z, yoy_change, global_z, rolling_z, second_deriv,
                    bool(r.get('is_z_anomaly', False)), bool(r.get('is_global_yoy_anomaly', False)),
                    bool(r.get('is_rolling_yoy_anomaly', False)), bool(r.get('is_acceleration_anomaly', False)),
                    bool(r.get('is_yoy_anomaly', False)), bool(r.get('is_increase_anomaly', False)),
                    bool(r.get('is_decrease_anomaly', False))
                ))
            if not self.connect_to_database():
                return False
            cursor = self.connection.cursor()
            try:
                cursor.executemany(insert, records)
                self.connection.commit()
                logger.info(f"Successfully saved {len(records)} anomalies to database")
                return True
            except Error as e:
                self.connection.rollback()
                logger.error(f"Failed to save anomalies: {str(e)}")
                return False
            finally:
                cursor.close()
        except Exception as e:
            logger.error(f"Error in save_anomalies_to_db: {str(e)}")
            return False

def create_population_anomalies_table(db_config):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS Population_Anomalies (
        anomaly_id INT AUTO_INCREMENT PRIMARY KEY,
        country_id INT NOT NULL,
        source_id INT NOT NULL,
        year INT NOT NULL,
        population_z FLOAT NULL,
        yoy_change FLOAT NULL,
        global_z_score FLOAT NULL,
        rolling_z FLOAT NULL,
        second_derivative FLOAT NULL,
        is_z_anomaly BOOLEAN NOT NULL DEFAULT 0,
        is_global_yoy_anomaly BOOLEAN NOT NULL DEFAULT 0,
        is_rolling_yoy_anomaly BOOLEAN NOT NULL DEFAULT 0,
        is_acceleration_anomaly BOOLEAN NOT NULL DEFAULT 0,
        is_yoy_anomaly BOOLEAN NOT NULL DEFAULT 0,
        is_increase_anomaly BOOLEAN NOT NULL DEFAULT 0,
        is_decrease_anomaly BOOLEAN NOT NULL DEFAULT 0,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        INDEX(country_id, year),
        INDEX(source_id, year),
        FOREIGN KEY(country_id) REFERENCES Countries(country_id),
        FOREIGN KEY(source_id) REFERENCES Data_Sources(source_id)
    );
    """
    connection = None
    cursor = None
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        cursor.execute(create_table_sql)
        connection.commit()
        logger.info("Population_Anomalies table created or already exists")
        return True
    except Error as e:
        logger.error(f"Error creating table: {str(e)}")
        return False
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

if __name__ == "__main__":
    db_config = {
        'user': 'root',
        'password': 'LZ#amhe!32',
        'host': '127.0.0.1',
        'database': 'fyp1',
        'raise_on_warnings': True
    }
    try:
        if not create_population_anomalies_table(db_config):
            logger.error("Failed to create/verify Population_Anomalies table. Exiting.")
            exit(1)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_dir = f"analysis_results_{timestamp}"
        analyzer = PopulationDataAnalyzer(db_config, results_dir)
        res = analyzer.analyze_population_data(1950, 2025)
        if res['status'] == 'success':
            print(f"Analysis completed successfully. Inserted {res['anomalies']} anomalies.")
        else:
            logger.info(f"Analysis completed with status: {res['status']}")
            if 'message' in res:
                print(f"Message: {res['message']}")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        print(f"An error occurred: {str(e)}")
    finally:
        try:
            if 'analyzer' in locals() and analyzer is not None:
                analyzer.close_connection()
        except:
            pass
