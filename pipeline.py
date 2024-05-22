import os
import logging
import pandas as pd

# Constants
LOCAL_DATA_PATH = './'
LOG_FILE = os.path.join(LOCAL_DATA_PATH, 'pipeline.log')
RAW_DATA_FILE = os.path.join(LOCAL_DATA_PATH, '2022-01-cheshire-street.csv')
OUTCOMES_DATA_FILE = os.path.join(LOCAL_DATA_PATH, '2022-01-cheshire-outcomes.csv')
STAGED_DATA_FILE = os.path.join(LOCAL_DATA_PATH, 'staged_cheshire_street.csv')
PRIMARY_DATA_FILE = os.path.join(LOCAL_DATA_PATH, 'primary_cheshire_street.csv')
REPORTING_DATA_FILE = os.path.join(LOCAL_DATA_PATH, 'reporting_cheshire_street.csv')




def ingest_data(file_path):
    """
    Ingest raw data from a CSV file.
    """
    logging.info(f"Starting data ingestion from {file_path}")
    if not os.path.exists(file_path):
        logging.error(f"File not found: {file_path}")
        return None

    try:
        df = pd.read_csv(file_path)
        logging.info(f"Data ingestion from {file_path} completed successfully")
        return df
    except ValueError as e:
        logging.error(f"Error reading the CSV file {file_path}: {e}")
        return None

def merge_data(df, df_outcomes):
    """
    Merge the main data with outcomes data on 'Crime ID'.
    """
    return pd.merge(df, df_outcomes[['Crime ID', 'Outcome type']], how='left', on='Crime ID')

def finaloutcome(df):
    """
    Create 'Final Outcome' column based on 'Outcome type' and 'Last outcome category'.
    """
    df['Final Outcome'] = df.apply(
        lambda row: row['Outcome type'] if pd.notnull(row['Outcome type']) else row['Last outcome category'],
        axis=1
    )
    return df

def categorize_outcome(outcome):
    if outcome in ['Unable to prosecute suspect', 
                   'Investigation complete; no suspect identified', 
                   'Status update unavailable']:
        return 'No Further Action'
    elif outcome in ['Local resolution', 
                     'Offender given a caution', 
                     'Action to be taken by another organisation', 
                     'Awaiting court outcome']:
        return 'Non-criminal Outcome'
    elif outcome in ['Further investigation is not in the public interest', 
                     'Further action is not in the public interest', 
                     'Formal action is not in the public interest']:
        return 'Public Interest Consideration'
    else:
        return 'Unknown'  # Or any other category for unknown outcomes

def apply_categorization(df):
    """
    Apply categorization to 'Final Outcome' column.
    """
    df['Broad Outcome Category'] = df['Final Outcome'].apply(categorize_outcome)
    return df

def del_values_street(df):
    """
    Delete unnecessary columns from the DataFrame.
    """
    cols_to_delete = ['Reported by', 'Context', 'Location', 'Last outcome category', 'Outcome type', 'Final Outcome']
    df.drop(columns=cols_to_delete, inplace=True)
    return df

def stage_data(df, df_outcomes, output_file):
    """
    Store the data to a CSV file for staging.
    """
    logging.info("Starting data staging")
    try:
        # Apply transformations
        df = merge_data(df, df_outcomes)
        df = finaloutcome(df)
        df = apply_categorization(df)
        df = del_values_street(df)

        # Save to CSV
        df.to_csv(output_file, index=False)
        logging.info("Data staging completed successfully")
    except Exception as e:
        logging.error(f"Error during data staging: {e}")

def primary_transformations(df):
    """
    Primary Storage Layer: Apply primary transformations to the data.
    """
    # Example transformation: Convert some columns to categorical data type
    df['Crime type'] = df['Crime type'].astype('category')
    df['Broad Outcome Category'] = df['Broad Outcome Category'].astype('category')

    # Example transformation: Create a new column by summing existing columns
    if 'Latitude' in df.columns and 'Longitude' in df.columns:
        df['Location Sum'] = df['Latitude'] + df['Longitude']

    return df

def primary_data(df, output_file):
    """
    Primary Storage Layer: Store the primary transformed data to a CSV file.
    """
    logging.info("Starting primary data transformation")
    try:
        # Apply primary transformations
        df = primary_transformations(df)

        # Save to CSV
        df.to_csv(output_file, index=False)
        logging.info("Primary data transformation completed successfully")
    except Exception as e:
        logging.error(f"Error during primary data transformation: {e}")

def reporting_aggregation(df):
    """
    Reporting Layer: Aggregate data for reporting purposes.
    """
    # Example aggregation: Count of crimes by crime type and broad outcome category
    agg_df = df.groupby(['Crime type', 'Broad Outcome Category']).size().reset_index(name='Count')

    return agg_df

def reporting_data(df, output_file):
    """
    Reporting Layer: Store the aggregated reporting data to a CSV file.
    """
    logging.info("Starting reporting data aggregation")
    try:
        # Apply aggregation
        agg_df = reporting_aggregation(df)

        # Save to CSV
        agg_df.to_csv(output_file, index=False)
        logging.info("Reporting data aggregation completed successfully")
    except Exception as e:
        logging.error(f"Error during reporting data aggregation: {e}")

def main():
    logging.info("Pipeline execution started")
    try:
        df = ingest_data(RAW_DATA_FILE)
        df_outcomes = ingest_data(OUTCOMES_DATA_FILE)
        
        if df is not None and df_outcomes is not None:
            stage_data(df, df_outcomes, STAGED_DATA_FILE)
            df_staged = ingest_data(STAGED_DATA_FILE)  # Read the staged data for further processing
            primary_data(df_staged, PRIMARY_DATA_FILE)
            df_primary = ingest_data(PRIMARY_DATA_FILE)  # Read the primary data for reporting
            reporting_data(df_primary, REPORTING_DATA_FILE)
        logging.info("Pipeline execution completed successfully")
    except Exception as e:
        logging.critical(f"Pipeline execution failed: {e}")

if __name__ == "__main__":
    main()

print( Hello Noah! )
