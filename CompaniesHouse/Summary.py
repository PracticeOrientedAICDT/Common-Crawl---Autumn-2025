import pandas as pd

# Load your CSV file
# Replace this path with the actual path to your file
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
print(df.head())

# Missing data summary
print("\n Missing values per column:")
print(df.isnull().sum())

# Unique stats 
print("\n📈 Example quick stats:")
if "CompanyName" in df.columns:
    print(f"Unique company names: {df['CompanyName'].nunique():,}")
if "CompanyStatus" in df.columns:
    print(df["CompanyStatus"].value_counts())
