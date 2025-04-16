"""
Demographic Data Validation Script

This script performs validation checks on a demographic database to ensure:
1. Data Completeness: All core indicators exist for each country-year-source combination
2. Sex-Specific Data Validation: Both male and female data exists for sex-disaggregated indicators
"""
import mysql.connector
from datetime import datetime
import sys
import csv
from typing import Dict, List, Tuple, Set

# Database configuration
config = {
    'user': 'root',       # MySQL username
    'password': 'LZ#amhe!32',   # MySQL password
    'host': '127.0.0.1',  # MySQL server host
    'database': 'fyp2',   # Database name
    'raise_on_warnings': True
}

class DemographicDataValidator:
    def __init__(self):
        """Initialize the database connection using the config."""
        try:
            self.conn = mysql.connector.connect(**config)
            self.cursor = self.conn.cursor(dictionary=True)
            print(f"Connected to database '{config['database']}' successfully.")
        except mysql.connector.Error as err:
            print(f"Error connecting to MySQL database: {err}")
            sys.exit(1)
            
    def __del__(self):
        """Close database connection when object is destroyed."""
        if hasattr(self, 'conn') and self.conn.is_connected():
            self.cursor.close()
            self.conn.close()
            print("Database connection closed.")
    
    def get_all_countries(self) -> List[Dict]:
        """Get all countries from the database."""
        self.cursor.execute("SELECT country_id, country_name FROM Countries")
        return self.cursor.fetchall()
    
    def get_all_sources(self) -> List[Dict]:
        """Get all data sources from the database."""
        self.cursor.execute("SELECT source_id, name FROM Data_Sources")
        return self.cursor.fetchall()
    
    def get_all_years(self) -> List[int]:
        """Get all unique years from the Population table as a representative dataset."""
        self.cursor.execute("SELECT DISTINCT year FROM Population ORDER BY year")
        years = self.cursor.fetchall()
        return [year['year'] for year in years]
    
    def get_core_indicators(self) -> List[Dict]:
        """Define core demographic indicators that should be present for all country-year-source combinations."""
        return [
            {"table": "Population", "column": "population"},
            {"table": "Birth_Rate", "column": "birth_rate"},
            {"table": "Death_Rate", "column": "death_rate"},
            {"table": "Fertility_Rate", "column": "Fertility_rate"},
            {"table": "Total_Net_Migration", "column": "net_migration"},
            {"table": "Crude_Net_Migration_Rate", "column": "migration_rate"},
            {"table": "Sex_Ratio_At_Birth", "column": "sex_ratio_at_birth"},
            {"table": "Sex_Ratio_Total_Population", "column": "sex_ratio"},
            {"table": "Median_Age", "column": "age"}
        ]
    
    def get_sex_specific_indicators(self) -> List[Dict]:
        """Define sex-specific demographic indicators."""
        return [
            {"table": "life_expectancy_at_birth_by_sex", "column": "life_expectancy"},
            {"table": "Infant_Mortality_Rate_By_Sex", "column": "infant_mortality_rate"},
            {"table": "Under_Five_Mortality_Rate_By_Sex", "column": "mortality_rate"},
            {"table": "Population_by_sex", "column": "population"}
        ]
    
    def check_data_completeness(self, output_file: str = "missing_indicators_report.csv"):
        """
        Check for missing core indicators for each country-year-source combination.
        
        Args:
            output_file: Path to save the CSV report of missing indicators
        """
        countries = self.get_all_countries()
        sources = self.get_all_sources()
        years = self.get_all_years()
        core_indicators = self.get_core_indicators()
        
        print(f"\n{'='*80}\nRunning Data Completeness Check\n{'='*80}")
        print(f"Checking {len(countries)} countries, {len(sources)} sources, {len(years)} years, and {len(core_indicators)} core indicators")
        
        missing_data = []
        total_checks = len(countries) * len(sources) * len(years) * len(core_indicators)
        completed = 0
        
        for country in countries:
            country_id = country['country_id']
            country_name = country['country_name']
            
            for source in sources:
                source_id = source['source_id']
                source_name = source['name']
                
                for year in years:
                    # Track which indicators exist for this combination
                    existing_indicators = set()
                    
                    for indicator in core_indicators:
                        table = indicator['table']
                        column = indicator['column']
                        
                        query = f"""
                        SELECT COUNT(*) as count 
                        FROM {table} 
                        WHERE country_id = %s AND source_id = %s AND year = %s
                        """
                        self.cursor.execute(query, (country_id, source_id, year))
                        result = self.cursor.fetchone()
                        
                        completed += 1
                        if completed % 1000 == 0:
                            print(f"Progress: {completed}/{total_checks} checks completed ({(completed/total_checks)*100:.2f}%)")
                        
                        if result['count'] == 0:
                            missing_data.append({
                                'country_id': country_id,
                                'country_name': country_name,
                                'source_id': source_id,
                                'source_name': source_name,
                                'year': year,
                                'missing_indicator': table,
                                'indicator_column': column
                            })
                        else:
                            existing_indicators.add(table)
                    
                    # If we have at least one indicator but not all, this is a partial record
                    # that deserves special attention
                    if existing_indicators and len(existing_indicators) < len(core_indicators):
                        for indicator in core_indicators:
                            if indicator['table'] not in existing_indicators:
                                missing_data.append({
                                    'country_id': country_id,
                                    'country_name': country_name,
                                    'source_id': source_id,
                                    'source_name': source_name,
                                    'year': year,
                                    'missing_indicator': indicator['table'],
                                    'indicator_column': indicator['column'],
                                    'partial_record': True
                                })
        
        # Save results to CSV
        if missing_data:
            try:
                with open(output_file, 'w', newline='') as csvfile:
                    fieldnames = ['country_id', 'country_name', 'source_id', 'source_name', 
                                'year', 'missing_indicator', 'indicator_column', 'partial_record']
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    for row in missing_data:
                        if 'partial_record' not in row:
                            row['partial_record'] = False
                        writer.writerow(row)
                
                print(f"\nData completeness check completed. Found {len(missing_data)} missing indicators.")
                print(f"Results saved to {output_file}")
                
                # Generate summary statistics
                missing_by_indicator = {}
                missing_by_country = {}
                missing_by_source = {}
                missing_by_year = {}
                
                for item in missing_data:
                    indicator = item['missing_indicator']
                    country = item['country_name']
                    source = item['source_name']
                    year = item['year']
                    
                    missing_by_indicator[indicator] = missing_by_indicator.get(indicator, 0) + 1
                    missing_by_country[country] = missing_by_country.get(country, 0) + 1
                    missing_by_source[source] = missing_by_source.get(source, 0) + 1
                    missing_by_year[year] = missing_by_year.get(year, 0) + 1
                
                print("\nSummary Statistics:")
                print(f"Missing indicators by table:")
                for indicator, count in sorted(missing_by_indicator.items(), key=lambda x: x[1], reverse=True):
                    print(f"  - {indicator}: {count}")
                
                print(f"\nTop 5 countries with missing data:")
                for country, count in sorted(missing_by_country.items(), key=lambda x: x[1], reverse=True)[:5]:
                    print(f"  - {country}: {count}")
                
                print(f"\nMissing data by source:")
                for source, count in sorted(missing_by_source.items(), key=lambda x: x[1], reverse=True):
                    print(f"  - {source}: {count}")
                
            except Exception as e:
                print(f"Error writing to CSV file: {e}")
        else:
            print("No missing indicators found! All core indicators are present for all country-year-source combinations.")
    
    def check_sex_specific_data(self, output_file: str = "missing_sex_data_report.csv"):
        """
        Check for imbalanced sex-specific data where one sex is represented but not the other.
        
        Args:
            output_file: Path to save the CSV report of missing sex-specific data
        """
        countries = self.get_all_countries()
        sources = self.get_all_sources()
        years = self.get_all_years()
        sex_indicators = self.get_sex_specific_indicators()
        
        print(f"\n{'='*80}\nRunning Sex-Specific Data Validation\n{'='*80}")
        print(f"Checking {len(countries)} countries, {len(sources)} sources, {len(years)} years, and {len(sex_indicators)} sex-specific indicators")
        
        imbalanced_data = []
        total_checks = len(countries) * len(sources) * len(years) * len(sex_indicators)
        completed = 0
        
        for country in countries:
            country_id = country['country_id']
            country_name = country['country_name']
            
            for source in sources:
                source_id = source['source_id']
                source_name = source['name']
                
                for year in years:
                    for indicator in sex_indicators:
                        table = indicator['table']
                        column = indicator['column']
                        
                        # Check for male data
                        male_query = f"""
                        SELECT COUNT(*) as count 
                        FROM {table} 
                        WHERE country_id = %s AND source_id = %s AND year = %s AND sex = 'Male'
                        """
                        self.cursor.execute(male_query, (country_id, source_id, year))
                        male_result = self.cursor.fetchone()
                        
                        # Check for female data
                        female_query = f"""
                        SELECT COUNT(*) as count 
                        FROM {table} 
                        WHERE country_id = %s AND source_id = %s AND year = %s AND sex = 'Female'
                        """
                        self.cursor.execute(female_query, (country_id, source_id, year))
                        female_result = self.cursor.fetchone()
                        
                        completed += 1
                        if completed % 1000 == 0:
                            print(f"Progress: {completed}/{total_checks} checks completed ({(completed/total_checks)*100:.2f}%)")
                        
                        # If we have data for one sex but not the other
                        if (male_result['count'] > 0 and female_result['count'] == 0) or \
                           (male_result['count'] == 0 and female_result['count'] > 0):
                            missing_sex = 'Female' if male_result['count'] > 0 else 'Male'
                            present_sex = 'Male' if male_result['count'] > 0 else 'Female'
                            
                            imbalanced_data.append({
                                'country_id': country_id,
                                'country_name': country_name,
                                'source_id': source_id, 
                                'source_name': source_name,
                                'year': year,
                                'indicator_table': table,
                                'indicator_column': column,
                                'missing_sex': missing_sex,
                                'present_sex': present_sex
                            })
        
        # Save results to CSV
        if imbalanced_data:
            try:
                with open(output_file, 'w', newline='') as csvfile:
                    fieldnames = ['country_id', 'country_name', 'source_id', 'source_name', 
                                'year', 'indicator_table', 'indicator_column', 
                                'missing_sex', 'present_sex']
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    for row in imbalanced_data:
                        writer.writerow(row)
                
                print(f"\nSex-specific data validation completed. Found {len(imbalanced_data)} instances of imbalanced sex data.")
                print(f"Results saved to {output_file}")
                
                # Generate summary statistics
                imbalanced_by_indicator = {}
                imbalanced_by_country = {}
                imbalanced_by_sex = {'Male': 0, 'Female': 0}
                
                for item in imbalanced_data:
                    indicator = item['indicator_table']
                    country = item['country_name']
                    missing_sex = item['missing_sex']
                    
                    imbalanced_by_indicator[indicator] = imbalanced_by_indicator.get(indicator, 0) + 1
                    imbalanced_by_country[country] = imbalanced_by_country.get(country, 0) + 1
                    imbalanced_by_sex[missing_sex] += 1
                
                print("\nSummary Statistics:")
                print(f"Imbalanced data by indicator:")
                for indicator, count in sorted(imbalanced_by_indicator.items(), key=lambda x: x[1], reverse=True):
                    print(f"  - {indicator}: {count}")
                
                print(f"\nTop 5 countries with imbalanced sex data:")
                for country, count in sorted(imbalanced_by_country.items(), key=lambda x: x[1], reverse=True)[:5]:
                    print(f"  - {country}: {count}")
                
                print(f"\nMissing data by sex:")
                for sex, count in imbalanced_by_sex.items():
                    print(f"  - {sex}: {count}")
                
            except Exception as e:
                print(f"Error writing to CSV file: {e}")
        else:
            print("No imbalanced sex-specific data found! All sex-specific indicators have both male and female data.")

    def run_all_validations(self):
        """Run all validation checks."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.check_data_completeness(f"missing_indicators_report_{timestamp}.csv")
        self.check_sex_specific_data(f"missing_sex_data_report_{timestamp}.csv")
        print("\nAll validation checks completed.")


if __name__ == "__main__":
    # No need for command line arguments as config is defined in the script
    validator = DemographicDataValidator()
    
    # Simple command line option to run specific validation if needed
    import sys
    if len(sys.argv) > 1:
        if sys.argv[1] == '--check-completeness':
            validator.check_data_completeness()
        elif sys.argv[1] == '--check-sex-data':
            validator.check_sex_specific_data()
        else:
            print(f"Unknown option: {sys.argv[1]}")
            print("Available options: --check-completeness, --check-sex-data")
            print("Running all validations by default...")
            validator.run_all_validations()
    else:
        validator.run_all_validations()