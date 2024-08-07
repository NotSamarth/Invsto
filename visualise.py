import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

# Database connection details
host = 'localhost'
user = 'root'
password = 'root'
database_name = 'invsto'

# Create a database URI
DATABASE_URI = f'mysql+pymysql://{user}:{password}@{host}:3306/{database_name}'

# Create an engine instance
engine = create_engine(DATABASE_URI)

# SQL query to fetch the required data
query = """
SELECT Date, `Low`
FROM stock_data
"""

# Load the data into a pandas DataFrame
df = pd.read_sql(query, engine)

# Convert the 'Date' column to datetime format
df['Date'] = pd.to_datetime(df['Date'])

# Set the 'Date' column as the index
df.set_index('Date', inplace=True)

# Resample the data to daily frequency, taking the first 'Open' value of each day
daily_open = df['Low'].resample('D').first()

# Plot the data
plt.figure(figsize=(12, 6))
plt.plot(daily_open.index, daily_open.values, label='Daily Low Prices')
plt.title('Daily Low Prices')
plt.xlabel('Date')
plt.ylabel('Low Price')
plt.legend()
plt.grid(True)
plt.show()