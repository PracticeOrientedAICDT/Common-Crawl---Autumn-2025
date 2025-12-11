import pandas as pd

# # Load your CSV file
file_path = "/Users/mm25873/Documents/Practice Project 1/Companies House data/BasicCompanyDataAsOneFile-2025-10-01.csv"

# Read only first few rows to inspect structure (fast)
preview = pd.read_csv(file_path, nrows=5)
print("\n Preview of CSV structure:")
print(preview)

# Read the full file
print("\n Loading full dataset (this may take a minute)...")
df = pd.read_csv(file_path)

# Basic summary
print("\n Dataset loaded successfully!")
print(f"Rows: {df.shape[0]:,}")
print(f"Columns: {df.shape[1]}")

# Column info
print("\n Column names and types:")
print(df.dtypes)

# Peek at the first few rows
print("\n First 5 rows:")
print(df.head(200))

# Missing data summary
print("\n Missing values per column:")
print(df.isnull().sum())

with pd.option_context('display.max_rows', None, 'display.max_colwidth', None):
    print(df.head(10))  # prints 10 rows fully, no truncation


# --- Convert dates ---
df['Accounts.LastMadeUpDate'] = pd.to_datetime(df['Accounts.LastMadeUpDate'], errors='coerce')
df['IncorporationDate'] = pd.to_datetime(df['IncorporationDate'], errors='coerce')

# --- Filter: active companies ---
df_active = df[df['CompanyStatus'] == "Active"].copy()

# --- Filter: accounts filed within last 12 months (proxy for actively trading) ---
one_year_ago = pd.Timestamp.today() - pd.DateOffset(years=1)
df_trading = df_active[df_active['Accounts.LastMadeUpDate'] >= one_year_ago].copy()

two_years_ago = pd.Timestamp.today() - pd.DateOffset(years=2)
df_trading_two = df_active[df_active['Accounts.LastMadeUpDate'] >= two_years_ago].copy()

print(f"Active companies: {len(df_active):,}")
print(f"Estimated actively trading: {len(df_trading):,}")
print(f"Estimated actively trading2: {len(df_trading_two):,}")

# Save for modelling
df_trading.to_parquet("companies_house_active_trading.parquet", index=False)
