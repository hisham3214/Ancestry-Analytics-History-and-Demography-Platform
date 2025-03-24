import mysql.connector
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
from typing import Dict, List, Tuple, Optional
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PopulationDataAnalyzer:
    def __init__(self, db_config: Dict[str, str]):
        """
        Initialize the Population Data Analyzer
        
        Parameters:
        -----------
        db_config : Dict[str, str]
            Database connection configuration
        """
        self.db_config = db_config
        self.connection = None
        self.source_mapping = {}
        self.country_mapping = {}

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
            
        query = "SELECT source_id, name FROM Data_Sources"
        df = pd.read_sql(query, self.connection)
        self.source_mapping = dict(zip(df['source_id'], df['name']))
        logger.info(f"Loaded {len(self.source_mapping)} data sources")
        return self.source_mapping
    
    def load_country_mapping(self) -> Dict[int, Tuple[str, str]]:
        """Load mapping of country_id to (country_name, country_code)"""
        if not self.connection:
            self.connect_to_database()
            
        query = "SELECT country_id, country_name, country_code FROM Countries"
        df = pd.read_sql(query, self.connection)
        self.country_mapping = {row['country_id']: (row['country_name'], row['country_code']) 
                              for _, row in df.iterrows()}
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
        
        # Load mappings if not already loaded
        if not self.source_mapping:
            self.load_source_mapping()
        if not self.country_mapping:
            self.load_country_mapping()
            
        # Define the country exclusion set with the specific country IDs to exclude
        country_exclusion_set = {299,298,297,296,295,291,289,288,287,286,285,284,283,282,281,280,279,278,277,276,275,274,273,272,271,270,269,268,267,266,265,264,263,262,261,259,258,257,256,255,254,253,252,251,250,249,248,247,246,245,244,243,242,241,240,239,238}
        
        # Convert set to comma-separated string for SQL query
        excluded_countries = ','.join(str(id) for id in country_exclusion_set)
        
        # Query to get population data from main Population table
        # Excluding source_id = 6 and specified country IDs
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
            population_df = pd.read_sql(query, self.connection)
            logger.info(f"Extracted {len(population_df)} population records")
            return population_df
        except mysql.connector.Error as err:
            logger.error(f"Failed to extract population data: {err}")
            raise

    def extract_population_by_sex(self, start_year: int = 1950, end_year: int = 2025) -> pd.DataFrame:
        """
        Extract population by sex data from the database
        
        Parameters:
        -----------
        start_year : int
            Starting year for data extraction
        end_year : int
            Ending year for data extraction
            
        Returns:
        --------
        pd.DataFrame
            DataFrame containing population by sex data
        """
        if not self.connection:
            self.connect_to_database()
            
        # Load mappings if not already loaded
        if not self.source_mapping:
            self.load_source_mapping()
        if not self.country_mapping:
            self.load_country_mapping()
            
        # Define the country exclusion set with the specific country IDs to exclude
        country_exclusion_set = {299,298,297,296,295,291,289,288,287,286,285,284,283,282,281,280,279,278,277,276,275,274,273,272,271,270,269,268,267,266,265,264,263,262,261,259,258,257,256,255,254,253,252,251,250,249,248,247,246,245,244,243,242,241,240,239,238}
        
        # Convert set to comma-separated string for SQL query
        excluded_countries = ','.join(str(id) for id in country_exclusion_set)
            
        # Query to get population data from Population_By_Sex table
        # Excluding source_id = 6 and specified country IDs
        query = f"""
        SELECT p.country_id, p.source_id, p.year, p.sex_id, p.sex, p.population,
               c.country_name, c.country_code, s.name as source_name
        FROM Population_By_Sex p
        JOIN Countries c ON p.country_id = c.country_id
        JOIN Data_Sources s ON p.source_id = s.source_id
        WHERE p.year BETWEEN {start_year} AND {end_year}
        AND p.source_id != 6
        AND p.country_id NOT IN ({excluded_countries})
        ORDER BY c.country_name, p.year, p.sex, s.name
        """
        
        try:
            logger.info(f"Extracting population by sex data from {start_year} to {end_year}")
            population_sex_df = pd.read_sql(query, self.connection)
            logger.info(f"Extracted {len(population_sex_df)} population by sex records")
            return population_sex_df
        except mysql.connector.Error as err:
            logger.error(f"Failed to extract population by sex data: {err}")
            raise

    def detect_anomalies_z_score(self, df: pd.DataFrame, 
                                threshold: float = 3.0) -> pd.DataFrame:
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
        
        # Group by country to calculate country-specific z-scores
        result_dfs = []
        
        for country_name, country_data in df.groupby('country_name'):
            # For each country, calculate z-scores for each source separately
            for source_name, source_data in country_data.groupby('source_name'):
                # Sort by year to ensure proper sequence
                source_data = source_data.sort_values('year')
                
                # Calculate z-score of population values
                if len(source_data) > 1:  # Need at least 2 points to calculate meaningful z-scores
                    source_data['population_z'] = stats.zscore(source_data['population'])
                    source_data['is_anomaly'] = abs(source_data['population_z']) > threshold
                    result_dfs.append(source_data)
                else:
                    # If only one data point, can't calculate z-score
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

    def detect_anomalies_yoy_change(self, df: pd.DataFrame, 
                                   decrease_threshold: float = -0.05, 
                                   increase_threshold: float = 0.10) -> pd.DataFrame:
        """
        Detect anomalies based on year-over-year population changes
        
        Parameters:
        -----------
        df : pd.DataFrame
            DataFrame containing population data
        decrease_threshold : float
            Threshold for significant population decrease (negative value)
        increase_threshold : float
            Threshold for significant population increase
            
        Returns:
        --------
        pd.DataFrame
            DataFrame with year-over-year changes and anomalies flagged
        """
        logger.info(f"Detecting anomalies using YoY change with thresholds: decrease {decrease_threshold}, increase {increase_threshold}")
        
        # Group by country and source to calculate YoY changes
        result_dfs = []
        
        for (country_name, source_name), group_data in df.groupby(['country_name', 'source_name']):
            # Sort by year to ensure proper sequence
            group_data = group_data.sort_values('year')
            
            # Calculate year-over-year change
            group_data['population_prev'] = group_data['population'].shift(1)
            group_data['yoy_change'] = (group_data['population'] - group_data['population_prev']) / group_data['population_prev']
            
            # Flag anomalies based on thresholds
            group_data['is_decrease_anomaly'] = group_data['yoy_change'] <= decrease_threshold
            group_data['is_increase_anomaly'] = group_data['yoy_change'] >= increase_threshold
            group_data['is_yoy_anomaly'] = group_data['is_decrease_anomaly'] | group_data['is_increase_anomaly']
            
            result_dfs.append(group_data)
        
        if result_dfs:
            result_df = pd.concat(result_dfs)
            decrease_count = result_df['is_decrease_anomaly'].sum()
            increase_count = result_df['is_increase_anomaly'].sum()
            logger.info(f"Detected {decrease_count} significant decreases and {increase_count} significant increases")
            return result_df
        else:
            logger.warning("No data available for YoY change anomaly detection")
            return pd.DataFrame()

    def detect_source_discrepancies(self, df: pd.DataFrame, 
                                   threshold: float = 0.10) -> pd.DataFrame:
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
        
        # Group by country and year to compare sources
        result_data = []
        
        for (country_name, year), group_data in df.groupby(['country_name', 'year']):
            # Skip if only one source
            if len(group_data) <= 1:
                continue
                
            # Calculate statistics across sources
            min_pop = group_data['population'].min()
            max_pop = group_data['population'].max()
            mean_pop = group_data['population'].mean()
            
            # Calculate max discrepancy percentage
            max_discrepancy_pct = (max_pop - min_pop) / min_pop if min_pop > 0 else 0
            
            # Calculate coefficient of variation (relative standard deviation)
            cv = group_data['population'].std() / mean_pop if mean_pop > 0 else 0
            
            # Flag if discrepancy exceeds threshold
            is_discrepancy = max_discrepancy_pct > threshold
            
            # Add summary row
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

    def plot_population_trend(self, df: pd.DataFrame, 
                             country_name: str, 
                             output_file: Optional[str] = None) -> None:
        """
        Plot population trend for a specific country
        
        Parameters:
        -----------
        df : pd.DataFrame
            DataFrame containing population data
        country_name : str
            Name of country to plot
        output_file : str, optional
            Path to save the plot, if None the plot is displayed
        """
        # Filter data for the specified country
        country_data = df[df['country_name'] == country_name]
        
        if country_data.empty:
            logger.warning(f"No data available for {country_name}")
            return
        
        plt.figure(figsize=(12, 6))
        
        # Plot data for each source separately
        for source_name, source_data in country_data.groupby('source_name'):
            source_data = source_data.sort_values('year')
            plt.plot(source_data['year'], source_data['population'], 
                    marker='o', linestyle='-', label=source_name)
        
        # If YoY anomalies are in the dataframe, highlight them
        if 'is_yoy_anomaly' in country_data.columns:
            anomalies = country_data[country_data['is_yoy_anomaly']]
            if not anomalies.empty:
                plt.scatter(anomalies['year'], anomalies['population'], 
                           color='red', s=100, label='YoY Anomalies', zorder=5)
        
        # If Z-score anomalies are in the dataframe, highlight them
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
        
        if output_file:
            plt.savefig(output_file, dpi=300, bbox_inches='tight')
            logger.info(f"Saved plot to {output_file}")
        else:
            plt.show()
            
    def plot_sex_ratio_trend(self, sex_df: pd.DataFrame, 
                            country_name: str, 
                            output_file: Optional[str] = None) -> None:
        """
        Plot population by sex trend for a specific country
        
        Parameters:
        -----------
        sex_df : pd.DataFrame
            DataFrame containing population by sex data
        country_name : str
            Name of country to plot
        output_file : str, optional
            Path to save the plot, if None the plot is displayed
        """
        # Filter data for the specified country
        country_data = sex_df[sex_df['country_name'] == country_name]
        
        if country_data.empty:
            logger.warning(f"No sex-specific data available for {country_name}")
            return
        
        plt.figure(figsize=(12, 6))
        
        # Plot male and female population for each source
        for source_name, source_data in country_data.groupby('source_name'):
            # Filter and sort the data
            male_data = source_data[source_data['sex'] == 'Male'].sort_values('year')
            female_data = source_data[source_data['sex'] == 'Female'].sort_values('year')
            
            if not male_data.empty and not female_data.empty:
                plt.plot(male_data['year'], male_data['population'], 
                        linestyle='-', label=f'Male ({source_name})')
                plt.plot(female_data['year'], female_data['population'], 
                        linestyle='--', label=f'Female ({source_name})')
        
        plt.title(f'Population by Sex: {country_name}')
        plt.xlabel('Year')
        plt.ylabel('Population')
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        if output_file:
            plt.savefig(output_file, dpi=300, bbox_inches='tight')
            logger.info(f"Saved sex ratio plot to {output_file}")
        else:
            plt.show()

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
            # Connect to database if not already connected
            if not self.connection:
                self.connect_to_database()
                
            # Extract data
            logger.info("Starting population data analysis")
            pop_df = self.extract_population_data(start_year, end_year)
            sex_df = self.extract_population_by_sex(start_year, end_year)
            
            if pop_df.empty:
                logger.warning("No population data available for analysis")
                return {"status": "error", "message": "No population data available"}
            
            # Run anomaly detection methods
            z_score_results = self.detect_anomalies_z_score(pop_df)
            yoy_results = self.detect_anomalies_yoy_change(pop_df)
            discrepancy_results = self.detect_source_discrepancies(pop_df)
            
            # Combine anomaly flags
            if not yoy_results.empty and not z_score_results.empty:
                # Merge the anomaly flags
                analysis_df = yoy_results.merge(
                    z_score_results[['country_id', 'source_id', 'year', 'is_anomaly']], 
                    on=['country_id', 'source_id', 'year'], 
                    how='outer'
                )
                
                # Create overall anomaly flag
                analysis_df['is_any_anomaly'] = (
                    analysis_df['is_yoy_anomaly'].fillna(False) | 
                    analysis_df['is_anomaly'].fillna(False)
                )
            else:
                # Use whichever result is available
                analysis_df = yoy_results if not yoy_results.empty else z_score_results
                if not analysis_df.empty:
                    analysis_df['is_any_anomaly'] = (
                        analysis_df['is_yoy_anomaly'].fillna(False) 
                        if 'is_yoy_anomaly' in analysis_df.columns 
                        else analysis_df['is_anomaly'].fillna(False)
                    )
            
            # Prepare result summary
            result = {
                "status": "success",
                "data": {
                    "population_data": pop_df.to_dict('records') if not pop_df.empty else [],
                    "population_by_sex": sex_df.to_dict('records') if not sex_df.empty else [],
                    "analysis_results": analysis_df.to_dict('records') if not analysis_df.empty else [],
                    "source_discrepancies": discrepancy_results.to_dict('records') if not discrepancy_results.empty else []
                },
                "summary": {
                    "total_countries": len(pop_df['country_name'].unique()) if not pop_df.empty else 0,
                    "total_sources": len(pop_df['source_name'].unique()) if not pop_df.empty else 0,
                    "year_range": f"{start_year}-{end_year}",
                    "anomaly_count": int(analysis_df['is_any_anomaly'].sum()) if not analysis_df.empty and 'is_any_anomaly' in analysis_df.columns else 0,
                    "discrepancy_count": int(discrepancy_results['is_discrepancy'].sum()) if not discrepancy_results.empty else 0
                }
            }
            
            logger.info(f"Analysis complete. Found {result['summary']['anomaly_count']} anomalies and {result['summary']['discrepancy_count']} source discrepancies")
            return result
            
        except Exception as e:
            logger.error(f"Error during population analysis: {str(e)}")
            return {"status": "error", "message": str(e)}
        finally:
            if self.connection:
                self.close_connection()


if __name__ == "__main__":
    db_config = {
        'user': 'root',
        'password': 'LZ#amhe!32',
        'host': '127.0.0.1',
        'database': 'fyp1',
        'raise_on_warnings': True
    }
    
    analyzer = PopulationDataAnalyzer(db_config)
    
    # Run complete analysis
    results = analyzer.analyze_population_data(start_year=1950, end_year=2025)
    
    if results["status"] == "success":
        print(f"Analysis complete. Found {results['summary']['anomaly_count']} anomalies.")
        
        # Example: Generate plots for specific countries
        if results["data"]["analysis_results"]:
            analysis_df = pd.DataFrame(results["data"]["analysis_results"])
            
            # Get countries with anomalies
            anomaly_countries = analysis_df[analysis_df['is_any_anomaly']]['country_name'].unique()
            
            for country in anomaly_countries[:5]:  # Plot first 5 countries with anomalies
                analyzer.plot_population_trend(analysis_df, country, f"{country}_population_trend.png")
                
                # Also plot sex ratio if data is available
                if results["data"]["population_by_sex"]:
                    sex_df = pd.DataFrame(results["data"]["population_by_sex"])
                    analyzer.plot_sex_ratio_trend(sex_df, country, f"{country}_sex_ratio_trend.png")
    else:
        print(f"Analysis failed: {results['message']}")