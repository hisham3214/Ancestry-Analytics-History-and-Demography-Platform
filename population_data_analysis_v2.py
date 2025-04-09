import mysql.connector
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy import stats
from typing import Dict, Tuple, Optional
import logging
import os
import json
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PopulationDataAnalyzer:
    def __init__(self, db_config: Dict[str, str], results_dir: str = "analysis_results"):
        """
        Initialize the Population Data Analyzer

        Parameters:
        -----------
        db_config : Dict[str, str]
            Database connection configuration
        results_dir : str
            Directory to save analysis results
        """
        self.db_config = db_config
        self.connection = None
        self.source_mapping = {}
        self.country_mapping = {}
        self.results_dir = results_dir

        # Create results directory if it doesn't exist
        if not os.path.exists(results_dir):
            os.makedirs(results_dir)
            logger.info(f"Created results directory: {results_dir}")

        # Create subdirectories for plots and data
        self.plots_dir = os.path.join(results_dir, "plots")
        self.data_dir = os.path.join(results_dir, "data")

        if not os.path.exists(self.plots_dir):
            os.makedirs(self.plots_dir)
            logger.info(f"Created plots directory: {self.plots_dir}")

        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
            logger.info(f"Created data directory: {self.data_dir}")

    def connect_to_database(self) -> None:
        """Establish connection to the database"""
        try:
            self.connection = mysql.connector.connect(**self.db_config)
            logger.info("Successfully connected to the database")
        except mysql.connector.Error as err:
            logger.error(f"Database connection failed: {err}")
            raise

    def close_connection(self) -> None:
        """Close the database connection"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.info("Database connection closed")

    def load_source_mapping(self) -> Dict[int, str]:
        """Load mapping of source_id to source name"""
        if not self.connection:
            self.connect_to_database()

        cursor = self.connection.cursor(dictionary=True)
        cursor.execute("SELECT source_id, name FROM Data_Sources")
        results = cursor.fetchall()
        cursor.close()

        self.source_mapping = {row['source_id']: row['name'] for row in results}
        logger.info(f"Loaded {len(self.source_mapping)} data sources")
        return self.source_mapping

    def load_country_mapping(self) -> Dict[int, Tuple[str, str]]:
        """Load mapping of country_id to (country_name, country_code)"""
        if not self.connection:
            self.connect_to_database()

        cursor = self.connection.cursor(dictionary=True)
        cursor.execute("SELECT country_id, country_name, country_code FROM Countries")
        results = cursor.fetchall()
        cursor.close()

        self.country_mapping = {row['country_id']: (row['country_name'], row['country_code'])
                                for row in results}
        logger.info(f"Loaded {len(self.country_mapping)} countries")
        return self.country_mapping

    def extract_population_data(self, start_year: int = 1950, end_year: int = 2025) -> pd.DataFrame:
        """
        Extract population data from the database

        Parameters:
        -----------
        start_year : int
            Starting year for data extraction
        end_year : int
            Ending year for data extraction

        Returns:
        --------
        pd.DataFrame
            DataFrame containing population data
        """
        if not self.connection:
            self.connect_to_database()

        if not self.source_mapping:
            self.load_source_mapping()
        if not self.country_mapping:
            self.load_country_mapping()

        country_exclusion_set = {
            299,298,297,296,295,291,289,288,287,286,285,284,283,282,
            281,280,279,278,277,276,275,274,273,272,271,270,269,268,
            267,266,265,264,263,262,261,259,258,257,256,255,254,253,
            252,251,250,249,248,247,246,245,244,243,242,241,240,239,238
        }

        excluded_countries = ','.join(str(cid) for cid in country_exclusion_set)

        query = f"""
        SELECT p.country_id, p.source_id, p.year, p.population, 
               c.country_name, c.country_code, s.name as source_name
        FROM Population p
        JOIN Countries c ON p.country_id = c.country_id
        JOIN Data_Sources s ON p.source_id = s.source_id
        WHERE p.year BETWEEN {start_year} AND {end_year}
        AND p.source_id != 6
        AND p.country_id NOT IN ({excluded_countries})
        ORDER BY c.country_name, p.year, s.name
        """

        try:
            logger.info(f"Extracting population data from {start_year} to {end_year}")
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute(query)
            records = cursor.fetchall()
            cursor.close()

            population_df = pd.DataFrame(records)

            raw_data_file = os.path.join(self.data_dir, "raw_population_data.csv")
            population_df.to_csv(raw_data_file, index=False)
            logger.info(f"Saved raw population data to {raw_data_file}")

            logger.info(f"Extracted {len(population_df)} population records")
            return population_df
        except mysql.connector.Error as err:
            logger.error(f"Failed to extract population data: {err}")
            raise

    def detect_anomalies_z_score(self, df: pd.DataFrame, threshold: float = 3.0) -> pd.DataFrame:
        """
        Detect anomalies using Z-score method

        Parameters:
        -----------
        df : pd.DataFrame
            DataFrame containing population data
        threshold : float
            Z-score threshold for anomaly detection

        Returns:
        --------
        pd.DataFrame
            DataFrame with anomalies flagged
        """
        logger.info(f"Detecting anomalies using Z-score with threshold {threshold}")
        result_dfs = []

        for country_name, country_data in df.groupby('country_name'):
            for source_name, source_data in country_data.groupby('source_name'):
                source_data = source_data.sort_values('year').copy()
                if len(source_data) > 1:
                    source_data['population_z'] = stats.zscore(source_data['population'])
                    source_data['is_anomaly'] = source_data['population_z'].abs() > threshold
                else:
                    source_data['population_z'] = 0
                    source_data['is_anomaly'] = False
                result_dfs.append(source_data)

        if result_dfs:
            result_df = pd.concat(result_dfs)
            anomaly_count = result_df['is_anomaly'].sum()
            logger.info(f"Detected {anomaly_count} anomalies using Z-score method")
            return result_df
        else:
            logger.warning("No data available for Z-score anomaly detection")
            return pd.DataFrame()

    def detect_anomalies_yoy_advanced(self, df: pd.DataFrame,
                                      z_threshold: float = 2.0,
                                      window_size: int = 5,
                                      second_deriv_threshold: float = 0.03) -> pd.DataFrame:
        """
        Advanced YoY anomaly detection combining multiple methods:
        1. Country-specific statistical thresholds
        2. Rolling window analysis
        3. Second derivative analysis
        4. Combined flag if any method detects an anomaly

        Parameters:
        -----------
        df : pd.DataFrame
            DataFrame containing population data
        z_threshold : float
            Number of standard deviations for anomaly detection
        window_size : int
            Size of the rolling window for local trend analysis
        second_deriv_threshold : float
            Threshold for second derivative (change in the change rate)

        Returns:
        --------
        pd.DataFrame
            DataFrame with comprehensive anomaly flags
        """
        logger.info("Detecting YoY anomalies using advanced methods")
        result_dfs = []

        # Group by (country, source) to handle each combination's patterns independently
        for (country_name, source_name), group_data in df.groupby(['country_name', 'source_name']):
            # Skip if insufficient data points
            if len(group_data) < max(3, window_size):
                continue

            # Sort by year, so shifts/rolling windows are correct
            group_data = group_data.sort_values('year').copy()

            # ----------------------------
            # 1. Year-over-Year (YoY) change
            # ----------------------------
            group_data['population_prev'] = group_data['population'].shift(1)
            # yoy_change = (Pop_t - Pop_(t-1)) / Pop_(t-1)
            group_data['yoy_change'] = ((group_data['population'] - group_data['population_prev'])
                                        / group_data['population_prev'])
            # Replace inf/-inf with NaN and then fill with 0
            group_data['yoy_change'] = group_data['yoy_change'].replace([np.inf, -np.inf], np.nan).fillna(0)

            # ----------------------------
            # 2. Country-specific global z-score
            #    (based on mean & std of *all* yoy_change for that country+source)
            # ----------------------------
            mean_change = group_data['yoy_change'].mean()
            std_change = group_data['yoy_change'].std()

            if std_change > 0:
                group_data['global_z_score'] = (group_data['yoy_change'] - mean_change) / std_change
                group_data['is_global_anomaly'] = abs(group_data['global_z_score']) > z_threshold
            else:
                # If std = 0, all yoy_change are identical → no global anomalies
                group_data['global_z_score'] = 0
                group_data['is_global_anomaly'] = False

            # ----------------------------
            # 3. Rolling Window Analysis (local z-score)
            # ----------------------------
            if len(group_data) >= window_size:
                group_data['rolling_mean'] = (
                    group_data['yoy_change']
                    .rolling(window=window_size, min_periods=2)
                    .mean()
                )
                group_data['rolling_std'] = (
                    group_data['yoy_change']
                    .rolling(window=window_size, min_periods=2)
                    .std()
                )

                # Calculate local z-score
                group_data['rolling_z'] = np.nan
                valid_mask = group_data['rolling_std'] > 0
                group_data.loc[valid_mask, 'rolling_z'] = (
                    (group_data.loc[valid_mask, 'yoy_change']
                     - group_data.loc[valid_mask, 'rolling_mean'])
                    / group_data.loc[valid_mask, 'rolling_std']
                )

                group_data['is_rolling_anomaly'] = abs(group_data['rolling_z']) > z_threshold
            else:
                # Not enough data for rolling window
                group_data['rolling_mean'] = np.nan
                group_data['rolling_std'] = np.nan
                group_data['rolling_z'] = np.nan
                group_data['is_rolling_anomaly'] = False

            # ----------------------------
            # 4. Second Derivative (acceleration/deceleration)
            # ----------------------------
            group_data['yoy_change_prev'] = group_data['yoy_change'].shift(1)
            group_data['second_derivative'] = group_data['yoy_change'] - group_data['yoy_change_prev']
            group_data['is_acceleration_anomaly'] = abs(group_data['second_derivative']) > second_deriv_threshold
            group_data['is_acceleration_anomaly'] = group_data['is_acceleration_anomaly'].fillna(False)

            # ----------------------------
            # 5. Combined flag
            # ----------------------------
            group_data['is_yoy_anomaly'] = (
                group_data['is_global_anomaly']
                | group_data['is_rolling_anomaly']
                | group_data['is_acceleration_anomaly']
            )

            # ----------------------------
            # 6. Distinguish between "increase" or "decrease" anomalies
            # ----------------------------
            group_data['is_decrease_anomaly'] = group_data['is_yoy_anomaly'] & (group_data['yoy_change'] < 0)
            group_data['is_increase_anomaly'] = group_data['is_yoy_anomaly'] & (group_data['yoy_change'] > 0)

            # ----------------------------
            # 7. Category and Description
            # ----------------------------
            group_data['anomaly_type'] = ''
            any_anomaly_mask = group_data['is_yoy_anomaly']
            if any_anomaly_mask.any():

                def categorize_anomaly(row):
                    methods = []
                    if row['is_global_anomaly']:
                        methods.append('global')
                    if row['is_rolling_anomaly']:
                        methods.append('local')
                    if row['is_acceleration_anomaly']:
                        methods.append('acceleration')
                    return '+'.join(methods) if methods else ''

                group_data.loc[any_anomaly_mask, 'anomaly_type'] = (
                    group_data.loc[any_anomaly_mask].apply(categorize_anomaly, axis=1)
                )

                def describe_anomaly(row):
                    year = row.get('year', 'unknown')
                    yoy_pct = row.get('yoy_change', 0) * 100
                    avg_pct = mean_change * 100  # compare to overall average yoy
                    if pd.isna(yoy_pct):
                        return f"Insufficient data for year {year}"

                    # Distinguish increase/decrease in text
                    change_type = "increase" if yoy_pct > 0 else "decrease"
                    relative_to_avg = "above" if yoy_pct > avg_pct else "below"
                    difference = abs(yoy_pct - avg_pct)

                    # Acceleration
                    accel_pct = (row.get('second_derivative', 0)) * 100
                    # If acceleration is large, mention it
                    if abs(accel_pct) > 1:
                        accel_type = "acceleration" if accel_pct > 0 else "deceleration"
                        return (f"Year {year}: {change_type} of {abs(yoy_pct):.1f}% "
                                f"({relative_to_avg} average by {difference:.1f}%), "
                                f"showing {accel_type} of {abs(accel_pct):.1f}%")
                    else:
                        return (f"Year {year}: {change_type} of {abs(yoy_pct):.1f}% "
                                f"({relative_to_avg} average by {difference:.1f}%)")

                group_data['anomaly_description'] = ''
                group_data.loc[any_anomaly_mask, 'anomaly_description'] = (
                    group_data.loc[any_anomaly_mask].apply(describe_anomaly, axis=1)
                )
            else:
                group_data['anomaly_description'] = ''

            result_dfs.append(group_data)

        if result_dfs:
            result_df = pd.concat(result_dfs)
            anomaly_count = result_df['is_yoy_anomaly'].sum()
            logger.info(f"Detected {anomaly_count} YoY anomalies using advanced methods")
            return result_df
        else:
            logger.warning("No data available for YoY change anomaly detection")
            return pd.DataFrame()

    def detect_source_discrepancies(self, df: pd.DataFrame, threshold: float = 0.10) -> pd.DataFrame:
        """
        Detect discrepancies between different data sources

        Parameters:
        -----------
        df : pd.DataFrame
            DataFrame containing population data
        threshold : float
            Threshold for significant discrepancy between sources (0.10 = 10%)

        Returns:
        --------
        pd.DataFrame
            DataFrame with source discrepancies flagged
        """
        logger.info(f"Detecting source discrepancies with threshold {threshold}")
        result_data = []

        for (country_name, year), group_data in df.groupby(['country_name', 'year']):
            if len(group_data) <= 1:
                continue

            min_pop = group_data['population'].min()
            max_pop = group_data['population'].max()
            mean_pop = group_data['population'].mean()

            max_discrepancy_pct = (max_pop - min_pop) / min_pop if min_pop > 0 else 0
            cv = group_data['population'].std() / mean_pop if mean_pop > 0 else 0

            is_discrepancy = max_discrepancy_pct > threshold

            result_data.append({
                'country_name': country_name,
                'year': year,
                'min_population': min_pop,
                'max_population': max_pop,
                'mean_population': mean_pop,
                'source_count': len(group_data),
                'max_discrepancy_pct': max_discrepancy_pct,
                'coefficient_of_variation': cv,
                'is_discrepancy': is_discrepancy,
                'sources': ', '.join(group_data['source_name'].unique())
            })

        if result_data:
            result_df = pd.DataFrame(result_data)
            discrepancy_count = result_df['is_discrepancy'].sum()
            logger.info(f"Detected {discrepancy_count} significant source discrepancies")
            return result_df
        else:
            logger.warning("No multi-source data available for discrepancy detection")
            return pd.DataFrame()

    def plot_population_trend(self, df: pd.DataFrame, country_name: str, save_to_file: bool = True) -> None:
        """
        Plot population trend for a specific country

        Parameters:
        -----------
        df : pd.DataFrame
            DataFrame containing population data
        country_name : str
            Name of country to plot
        save_to_file : bool
            Whether to save the plot to a file (True) or display it (False)
        """
        country_data = df[df['country_name'] == country_name]

        if country_data.empty:
            logger.warning(f"No data available for {country_name}")
            return

        plt.figure(figsize=(12, 6))
        for source_name, source_data in country_data.groupby('source_name'):
            source_data = source_data.sort_values('year')
            plt.plot(source_data['year'], source_data['population'],
                     marker='o', linestyle='-', label=source_name)

        # Plot anomalies if present
        if 'is_yoy_anomaly' in country_data.columns:
            anomalies = country_data[country_data['is_yoy_anomaly']]
            if not anomalies.empty:
                plt.scatter(anomalies['year'], anomalies['population'],
                            color='red', s=100, label='YoY Anomalies', zorder=5)

        if 'is_anomaly' in country_data.columns:
            z_anomalies = country_data[country_data['is_anomaly']]
            if not z_anomalies.empty:
                plt.scatter(z_anomalies['year'], z_anomalies['population'],
                            color='orange', s=80, marker='s', label='Z-score Anomalies', zorder=5)

        plt.title(f'Population Trend: {country_name}')
        plt.xlabel('Year')
        plt.ylabel('Population')
        plt.legend()
        plt.grid(True, alpha=0.3)

        safe_filename = country_name.replace(' ', '_').replace('/', '_')
        if save_to_file:
            output_file = os.path.join(self.plots_dir, f"{safe_filename}_population_trend.png")
            plt.savefig(output_file, dpi=300, bbox_inches='tight')
            plt.close()
            logger.info(f"Saved plot to {output_file}")

            csv_file = os.path.join(self.data_dir, f"{safe_filename}_population_data.csv")
            country_data.to_csv(csv_file, index=False)

            text_file = os.path.join(self.data_dir, f"{safe_filename}_analysis.txt")
            with open(text_file, 'w') as f:
                f.write(f"Population Analysis for {country_name}\n")
                f.write("=" * 50 + "\n\n")
                f.write("Basic Statistics:\n")
                f.write(f"First year: {country_data['year'].min()}\n")
                f.write(f"Last year: {country_data['year'].max()}\n")
                f.write(f"Number of data sources: {country_data['source_name'].nunique()}\n")
                latest_pop = country_data.loc[country_data['year'] == country_data['year'].max(), 'population'].mean()
                f.write(f"Latest population: {latest_pop:,.0f}\n\n")

                # Z-score anomalies
                if 'is_anomaly' in country_data.columns:
                    z_anomalies = country_data[country_data['is_anomaly']]
                    f.write(f"Z-Score Anomalies ({len(z_anomalies)}):\n")
                    if not z_anomalies.empty and 'population_z' in z_anomalies.columns:
                        for _, row in z_anomalies.iterrows():
                            pop_z_str = "N/A"
                            if not pd.isna(row.get('population_z', np.nan)):
                                pop_z_str = f"{row['population_z']:.2f}"
                            f.write(
                                f"Year {row['year']}: Population {row['population']:,.0f}, "
                                f"Z-score: {pop_z_str}, Source: {row['source_name']}\n"
                            )
                    else:
                        f.write("No Z-score anomalies detected or missing population_z.\n")
                    f.write("\n")

                # YoY anomalies
                if 'is_yoy_anomaly' in country_data.columns:
                    yoy_anomalies = country_data[country_data['is_yoy_anomaly']]
                    f.write(f"Year-over-Year Anomalies ({len(yoy_anomalies)}):\n")

                    if not yoy_anomalies.empty:
                        for _, row in yoy_anomalies.iterrows():
                            change_type = "increase" if row.get('is_increase_anomaly', False) else "decrease"
                            yoy_pct = row.get('yoy_change', 0) * 100
                            g_z = row.get('global_z_score', float('nan'))
                            r_z = row.get('rolling_z', float('nan'))
                            second_deriv = row.get('second_derivative', float('nan')) * 100
                            anomaly_desc = row.get('anomaly_description', '')
                            anomaly_type = row.get('anomaly_type', '')

                            f.write(
                                f"Year {row['year']}: {change_type} of {yoy_pct:.2f}% "
                                f"(Global z={g_z:.2f}, Rolling z={r_z:.2f}, 2nd deriv={second_deriv:.2f}), "
                                f"Type(s): {anomaly_type}, Description: {anomaly_desc}, "
                                f"Source: {row['source_name']}\n"
                            )
                else:
                    f.write("No YoY anomalies detected.\n")
        else:
            plt.show()

    def plot_z_anomalies(self, df: pd.DataFrame, country_name: str, save_to_file: bool = True) -> None:
        """
        Plot Z-score anomalies for a specific country and save details to a text file.
        """
        country_data = df[df['country_name'] == country_name]
        if 'is_anomaly' not in country_data.columns:
            logger.warning("No Z-score anomaly data available")
            return
        z_anomalies = country_data[country_data['is_anomaly']]
        if z_anomalies.empty:
            logger.info(f"No Z-score anomalies for {country_name}")
            return

        plt.figure(figsize=(10, 5))
        plt.plot(country_data['year'], country_data['population'],
                 marker='o', linestyle='-', label='Population')
        plt.scatter(z_anomalies['year'], z_anomalies['population'],
                    color='orange', s=100, marker='s', label='Z Anomaly', zorder=5)
        plt.title(f"Z-Score Anomalies for {country_name}")
        plt.xlabel("Year")
        plt.ylabel("Population")
        plt.legend()
        plt.grid(True, alpha=0.3)

        safe_filename = country_name.replace(' ', '_').replace('/', '_')
        if save_to_file:
            output_file = os.path.join(self.plots_dir, f"{safe_filename}_z_anomalies.png")
            plt.savefig(output_file, dpi=300, bbox_inches='tight')
            plt.close()
            logger.info(f"Saved Z-score anomaly plot to {output_file}")

            text_file = os.path.join(self.data_dir, f"{safe_filename}_z_anomalies.txt")
            with open(text_file, 'w') as f:
                f.write(f"Z-Score Anomalies for {country_name}\n")
                f.write("="*40 + "\n")
                for _, row in z_anomalies.iterrows():
                    z_val = "N/A"
                    if not pd.isna(row.get('population_z', np.nan)):
                        z_val = f"{row['population_z']:.2f}"
                    f.write(
                        f"Year {row['year']}: Population {row['population']:,.0f}, "
                        f"Z-score: {z_val}, Source: {row['source_name']}\n"
                    )
            logger.info(f"Saved Z-score anomaly details to {text_file}")
        else:
            plt.show()

    def plot_yoy_anomalies(self, df: pd.DataFrame, country_name: str, save_to_file: bool = True) -> None:
        """
        Plot YoY anomalies for a specific country and save details to a text file.
        """
        country_data = df[df['country_name'] == country_name]
        if 'is_yoy_anomaly' not in country_data.columns:
            logger.warning("No YoY anomaly data available")
            return
        yoy_anomalies = country_data[country_data['is_yoy_anomaly']]
        if yoy_anomalies.empty:
            logger.info(f"No YoY anomalies for {country_name}")
            return

        plt.figure(figsize=(10, 5))
        plt.plot(country_data['year'], country_data['population'],
                 marker='o', linestyle='-', label='Population')
        plt.scatter(yoy_anomalies['year'], yoy_anomalies['population'],
                    color='red', s=100, marker='^', label='YoY Anomaly', zorder=5)
        plt.title(f"Year-over-Year Anomalies for {country_name}")
        plt.xlabel("Year")
        plt.ylabel("Population")
        plt.legend()
        plt.grid(True, alpha=0.3)

        safe_filename = country_name.replace(' ', '_').replace('/', '_')
        if save_to_file:
            output_file = os.path.join(self.plots_dir, f"{safe_filename}_yoy_anomalies.png")
            plt.savefig(output_file, dpi=300, bbox_inches='tight')
            plt.close()
            logger.info(f"Saved YoY anomaly plot to {output_file}")

            text_file = os.path.join(self.data_dir, f"{safe_filename}_yoy_anomalies.txt")
            with open(text_file, 'w') as f:
                f.write(f"Year-over-Year Anomalies for {country_name}\n")
                f.write("="*40 + "\n")
                for _, row in yoy_anomalies.iterrows():
                    change_type = "increase" if row.get('is_increase_anomaly', False) else "decrease"
                    f.write(
                        f"Year {row['year']}: {change_type} of "
                        f"{row['yoy_change']*100:.1f}%, Population {row['population']:,.0f}, "
                        f"Source: {row['source_name']}\n"
                    )
            logger.info(f"Saved YoY anomaly details to {text_file}")
        else:
            plt.show()

    def save_anomalies_to_db(self, anomaly_df: pd.DataFrame) -> None:
        """
        Saves anomaly records to the `Population_Anomalies` table in the database.

        Expects the table to exist. Structure example:
            CREATE TABLE IF NOT EXISTS Population_Anomalies (
              anomaly_id INT AUTO_INCREMENT PRIMARY KEY,
              country_id INT NOT NULL,
              source_id INT NOT NULL,
              year INT NOT NULL,
              anomaly_type VARCHAR(255),
              anomaly_description TEXT,
              yoy_change FLOAT NULL,
              population_z FLOAT NULL,
              is_increase_anomaly TINYINT(1) DEFAULT 0,
              is_decrease_anomaly TINYINT(1) DEFAULT 0,
              created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """
        if anomaly_df.empty:
            logger.info("No anomalies to save to the database.")
            return

        if not self.connection:
            self.connect_to_database()

        insert_query = """
            INSERT INTO Population_Anomalies 
            (country_id, source_id, year, anomaly_type, anomaly_description,
             yoy_change, population_z, is_increase_anomaly, is_decrease_anomaly)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        try:
            cursor = self.connection.cursor()
            records_to_insert = []

            for _, row in anomaly_df.iterrows():
                # Safely retrieve fields (some might not be present in all rows)
                country_id = int(row['country_id'])
                source_id = int(row['source_id'])
                year = int(row['year'])
                anomaly_type = row.get('anomaly_type', '')
                anomaly_description = row.get('anomaly_description', '')

                yoy_change = row.get('yoy_change', None)
                population_z = row.get('population_z', None)

                is_increase_anomaly = 1 if row.get('is_increase_anomaly', False) else 0
                is_decrease_anomaly = 1 if row.get('is_decrease_anomaly', False) else 0

                records_to_insert.append((
                    country_id, source_id, year,
                    anomaly_type, anomaly_description,
                    yoy_change, population_z,
                    is_increase_anomaly, is_decrease_anomaly
                ))

            cursor.executemany(insert_query, records_to_insert)
            self.connection.commit()
            logger.info(f"Inserted {cursor.rowcount} anomaly records into Population_Anomalies table.")

        except mysql.connector.Error as err:
            self.connection.rollback()
            logger.error(f"Error inserting anomalies into Population_Anomalies: {err}")
        finally:
            cursor.close()

    def analyze_population_data(self, start_year: int = 1950, end_year: int = 2025) -> Dict:
        """
        Run a comprehensive analysis on population data

        Parameters:
        -----------
        start_year : int
            Starting year for analysis
        end_year : int
            Ending year for analysis

        Returns:
        --------
        Dict
            Dictionary containing analysis results
        """
        try:
            if not self.connection:
                self.connect_to_database()

            logger.info("Starting population data analysis")
            pop_df = self.extract_population_data(start_year, end_year)

            if pop_df.empty:
                logger.warning("No population data available for analysis")
                return {"status": "error", "message": "No population data available"}

            z_score_results = self.detect_anomalies_z_score(pop_df)
            yoy_results = self.detect_anomalies_yoy_advanced(pop_df)
            discrepancy_results = self.detect_source_discrepancies(pop_df)

            # Merge yoy and z, but also include 'population_z'
            if not yoy_results.empty and not z_score_results.empty:
                analysis_df = yoy_results.merge(
                    z_score_results[['country_id', 'source_id', 'year', 'is_anomaly', 'population_z']],
                    on=['country_id', 'source_id', 'year'],
                    how='outer'
                )
                analysis_df['is_any_anomaly'] = (
                    analysis_df['is_yoy_anomaly'].fillna(False) |
                    analysis_df['is_anomaly'].fillna(False)
                )
            else:
                # If one is empty, just use whichever isn't empty
                analysis_df = yoy_results if not yoy_results.empty else z_score_results
                if not analysis_df.empty:
                    # Ensure population_z column exists, even if it's all NaN
                    if 'population_z' not in analysis_df.columns:
                        analysis_df['population_z'] = np.nan
                    analysis_df['is_any_anomaly'] = (
                        analysis_df['is_yoy_anomaly'].fillna(False)
                        if 'is_yoy_anomaly' in analysis_df.columns
                        else analysis_df['is_anomaly'].fillna(False)
                    )

            # Save final data
            pop_file = os.path.join(self.data_dir, "all_population_data.csv")
            pop_df.to_csv(pop_file, index=False)

            if not analysis_df.empty:
                analysis_file = os.path.join(self.data_dir, "all_analysis_results.csv")
                analysis_df.to_csv(analysis_file, index=False)

            if not discrepancy_results.empty:
                discrepancy_file = os.path.join(self.data_dir, "source_discrepancies.csv")
                discrepancy_results.to_csv(discrepancy_file, index=False)

            # Write summary
            summary_file = os.path.join(self.data_dir, "analysis_summary.txt")
            with open(summary_file, 'w') as f:
                f.write("Population Data Analysis Summary\n")
                f.write("==============================\n\n")
                f.write(f"Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Date Range: {start_year} to {end_year}\n\n")
                f.write("Data Summary:\n")
                f.write(f"Total countries analyzed: {pop_df['country_name'].nunique()}\n")
                f.write(f"Total data sources: {pop_df['source_name'].nunique()}\n")
                f.write(f"Total data points: {len(pop_df)}\n\n")

                f.write("Anomaly Detection Results:\n")
                if not z_score_results.empty:
                    f.write(f"Z-score anomalies: {z_score_results['is_anomaly'].sum()}\n")
                if not yoy_results.empty:
                    f.write(f"Significant decreases: {yoy_results['is_decrease_anomaly'].sum()}\n")
                    f.write(f"Significant increases: {yoy_results['is_increase_anomaly'].sum()}\n")

                if not analysis_df.empty and 'is_any_anomaly' in analysis_df.columns:
                    anomaly_countries_count = analysis_df[analysis_df['is_any_anomaly']]['country_name'].nunique()
                    f.write(f"Total countries with anomalies: {anomaly_countries_count}\n\n")

                if not discrepancy_results.empty:
                    f.write("Source Discrepancy Summary:\n")
                    f.write(f"Total country-year combinations with multiple sources: {len(discrepancy_results)}\n")
                    f.write(f"Significant discrepancies: {discrepancy_results['is_discrepancy'].sum()}\n")
                    country_discrepancies = (
                        discrepancy_results[discrepancy_results['is_discrepancy']]
                        .groupby('country_name').size()
                        .sort_values(ascending=False)
                    )
                    if not country_discrepancies.empty:
                        f.write("\nTop 10 Countries with Most Source Discrepancies:\n")
                        for i, (country, count) in enumerate(country_discrepancies.head(10).items()):
                            f.write(f"{i+1}. {country}: {count} years with significant discrepancies\n")

            # Plot anomalies by country
            if not analysis_df.empty:
                anomaly_countries = analysis_df[analysis_df['is_any_anomaly']]['country_name'].unique()
                anomaly_summary_file = os.path.join(self.data_dir, "countries_with_anomalies.txt")
                with open(anomaly_summary_file, 'w') as f:
                    f.write("Countries with Population Anomalies\n")
                    f.write("===============================\n\n")
                    for country in sorted(anomaly_countries):
                        country_anomalies = analysis_df[
                            (analysis_df['country_name'] == country) &
                            analysis_df['is_any_anomaly']
                        ]
                        f.write(f"{country}: {len(country_anomalies)} anomalies\n")

                # Generate plots for countries with anomalies
                for country in anomaly_countries:
                    self.plot_population_trend(analysis_df, country)
                    self.plot_z_anomalies(analysis_df, country)
                    self.plot_yoy_anomalies(analysis_df, country)

                # -------------- NEW: SAVE ANOMALIES TO DB --------------
                # Filter out only anomaly rows
                anomalies_to_save = analysis_df[analysis_df['is_any_anomaly'] == True].copy()
                # Insert into the database table `Population_Anomalies`
                if not anomalies_to_save.empty:
                    self.save_anomalies_to_db(anomalies_to_save)

            result = {
                "status": "success",
                "data": {
                    "population_data": pop_df.to_dict('records') if not pop_df.empty else [],
                    "analysis_results": analysis_df.to_dict('records') if not analysis_df.empty else [],
                    "source_discrepancies": discrepancy_results.to_dict('records') if not discrepancy_results.empty else []
                },
                "summary": {
                    "total_countries": pop_df['country_name'].nunique() if not pop_df.empty else 0,
                    "total_sources": pop_df['source_name'].nunique() if not pop_df.empty else 0,
                    "year_range": f"{start_year}-{end_year}",
                    "anomaly_count": int(analysis_df['is_any_anomaly'].sum()) if not analysis_df.empty and 'is_any_anomaly' in analysis_df.columns else 0,
                    "discrepancy_count": int(discrepancy_results['is_discrepancy'].sum()) if not discrepancy_results.empty else 0,
                    "results_directory": self.results_dir
                }
            }

            # Create a brief JSON summary
            results_json = os.path.join(self.data_dir, "analysis_results.json")
            with open(results_json, 'w') as f:
                serializable_result = {
                    "status": result["status"],
                    "summary": result["summary"],
                }
                json.dump(serializable_result, f, indent=2)

            logger.info(
                f"Analysis complete. Found {result['summary']['anomaly_count']} anomalies "
                f"and {result['summary']['discrepancy_count']} source discrepancies"
            )
            logger.info(f"Results saved to {self.results_dir}")
            return result

        except Exception as e:
            logger.error(f"Error during population analysis: {str(e)}")
            return {"status": "error", "message": str(e)}
        finally:
            if self.connection:
                self.close_connection()

# Example usage:
if __name__ == "__main__":
    db_config = {
        'user': 'root',
        'password': 'LZ#amhe!32',
        'host': '127.0.0.1',
        'database': 'fyp1',
        'raise_on_warnings': True
    }
        # Create and execute the SQL
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS Population_Anomalies (
    anomaly_id INT AUTO_INCREMENT PRIMARY KEY,
    country_id INT NOT NULL,
    source_id INT NOT NULL,
    year INT NOT NULL,
    anomaly_type VARCHAR(255),
    anomaly_description TEXT,
    yoy_change FLOAT NULL,
    population_z FLOAT NULL,
    is_increase_anomaly TINYINT(1) DEFAULT 0,
    is_decrease_anomaly TINYINT(1) DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    """

    try:
        # Connect to MySQL
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Execute CREATE TABLE statement
        cursor.execute(create_table_sql)
        print("Table Population_Anomalies created or already exists.")

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        # Close cursor and connection
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_dir = f"analysis_results_{timestamp}"

    analyzer = PopulationDataAnalyzer(db_config, results_dir)
    results = analyzer.analyze_population_data(start_year=1950, end_year=2025)

    if results["status"] == "success":
        print(f"Analysis complete. Found {results['summary']['anomaly_count']} anomalies.")
        print(f"Results saved to: {results_dir}")
    else:
        print(f"Analysis failed: {results['message']}")
