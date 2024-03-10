import pandas as pd
import numpy as np
from datetime import datetime
from scipy.stats import zscore
import pymysql
from sqlalchemy import create_engine

# Data Ingestion
def ingest_data(file_path):
    # Assuming CSV format for now, can be extended for other formats
    df = pd.read_csv(file_path)
    return df

# Data Cleaning
def clean_data(df):
    # Check for missing values before cleaning
    print("Missing values before cleaning:")
    print(df.isnull().sum())
    
    # Remove rows with missing values
    df = df.dropna()
    
    # Check for missing values after cleaning
    print("Missing values after cleaning:")
    print(df.isnull().sum())

    # Print rows with missing values
    print("Rows with missing values:")
    print(df[df.isnull().any(axis=1)])
    
    # Identify and correct outliers using z-score method
    z_scores = np.abs(zscore(df[['Open', 'High', 'Low', 'Close']]))
    df = df[(z_scores < 3).all(axis=1)]  # Keeping only data within 3 standard deviations
    
    # Convert timestamp to datetime format
    df['Date'] = pd.to_datetime(df['Date'])
    
    return df

# Data Transformation
def calculate_technical_indicators(df):
    # Assuming 'Date' column is already in datetime format
    df.set_index('Date', inplace=True)  # Set 'Date' column as index
    
    # Example: Calculate Moving Average
    df['MA_50'] = df['Close'].rolling(window=50, min_periods=1).mean()  # Added min_periods=1 to handle missing values
    
    # Example: Calculate Bollinger Bands
    window = 20
    df['BB_middle'] = df['Close'].rolling(window=window, min_periods=1).mean()  # Added min_periods=1 to handle missing values
    df['BB_std'] = df['Close'].rolling(window=window, min_periods=1).std()  # Added min_periods=1 to handle missing values
    df['BB_upper'] = df['BB_middle'] + 2 * df['BB_std']
    df['BB_lower'] = df['BB_middle'] - 2 * df['BB_std']
    
    # Example: Calculate Relative Strength Index (RSI)
    delta = df['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14, min_periods=1).mean()  # Added min_periods=1 to handle missing values
    loss = (-delta.where(delta < 0, 0)).rolling(window=14, min_periods=1).mean()  # Added min_periods=1 to handle missing values
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    df['RSI'] = rsi
    
    # Resample the data to desired frequency (e.g., daily to hourly)
    df = df.resample('h').ffill()
    
    return df

# Data Validation (Unit Tests)
def test_data_integrity(df):
    # Check for non-numeric values in numeric columns
    numeric_cols = df.select_dtypes(include=np.number).columns
    non_numeric_values = df[numeric_cols].apply(lambda x: x[~x.apply(np.isreal)]).dropna()
    
    if not non_numeric_values.empty:
        print("Non-numeric values present in the following columns:")
        print(non_numeric_values)
        raise AssertionError("Non-numeric values present in the dataset")

# Data Storage
# Data Storage
def store_data(df, host, user, password, database_name):
    # Create SQLAlchemy engine
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{database_name}")
    
    # Assuming MySQL table named 'stock_data'
    df.to_sql('stock_data', con=engine, if_exists='replace', index=False)

    print("Data stored in MySQL successfully!")


# Main function to execute the pipeline
def main():
    # Step 1: Data Ingestion
    file_path = 'TATAMTRDVR.NS.csv'  # Replace with actual file path
    df = ingest_data(file_path)
    
    # Step 2: Data Cleaning
    df = clean_data(df)
    
    # Step 3: Data Transformation
    df = calculate_technical_indicators(df)
    
    # Reset index before data validation
    df.reset_index(inplace=True)
    
    # Step 4: Data Validation
    test_data_integrity(df)
    
    # Step 5: Data Storage
    host = 'localhost'  # Replace with actual host
    user = 'root'   # Replace with actual username
    password = 'root'   # Replace with actual password
    database_name = 'invsto'  # Replace with actual database name
    store_data(df, host, user, password, database_name)

if __name__ == "__main__":
    main()
