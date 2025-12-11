import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# ==========================
# Load datasets from CSV
# ==========================

ch_path = "/Users/mm25873/Documents/Practice Project 1/Companies House data/BasicCompanyDataAsOneFile-2025-10-01.csv"
crawl_path = "/Users/mm25873/Documents/Practice Project 1/Common Crawl Data/df2024.csv"       

print("Loading Companies House data...")
df_ch = pd.read_csv(ch_path)

print("Loading Common Crawl data...")
df_crawl = pd.read_csv(crawl_path)

output_dir = Path("/Users/mm25873/Documents/Practice Project 1/dataset_visuals")
output_dir.mkdir(exist_ok=True)

print("\nDatasets loaded.")
print("\nCompanies House columns:", df_ch.columns.tolist())
print("\nCommon Crawl columns:", df_crawl.columns.tolist())


# ==========================
# Helper: save figure
# ==========================

def savefig(name):
    plt.tight_layout()
    plt.savefig(output_dir / f"{name}.png", dpi=300)
    plt.close()


# ==========================
# Companies House Visuals
# ==========================

print("\nGenerating Companies House visuals...")

# Convert dates
date_cols = [
    "IncorporationDate",
    "Accounts.LastMadeUpDate",
    "Returns.LastMadeUpDate"
]

for col in date_cols:
    if col in df_ch.columns:
        df_ch[col] = pd.to_datetime(df_ch[col], errors="coerce")

# --- 1. Company age distribution ---
if "IncorporationDate" in df_ch.columns:
    df_ch["company_age_years"] = (
        (pd.Timestamp.today() - df_ch["IncorporationDate"]).dt.days / 365
    )

    plt.figure(figsize=(8, 5))
    sns.histplot(df_ch["company_age_years"].dropna(), bins=50)
    plt.title("Company Age Distribution (Years)")
    plt.xlabel("Years")
    plt.ylabel("Count")
    savefig("ch_company_age_distribution")

# --- 2. Top SIC codes ---
sic_col = "SICCode.SicText_1"
if sic_col in df_ch.columns:
    df_ch["primary_sic"] = df_ch[sic_col].astype(str).str.extract(r'(^\d{4})')

    plt.figure(figsize=(10, 6))
    df_ch["primary_sic"].value_counts().head(20).plot(kind="bar")
    plt.title("Top 20 SIC Codes")
    plt.xlabel("SIC Code")
    plt.ylabel("Number of Companies")
    savefig("ch_top_sic_codes")

# --- 3. Incorporation year ---
if "IncorporationDate" in df_ch.columns:
    df_ch["inc_year"] = df_ch["IncorporationDate"].dt.year

    plt.figure(figsize=(10, 5))
    df_ch["inc_year"].value_counts().sort_index().plot()
    plt.title("Incorporation Year Distribution")
    plt.xlabel("Year")
    plt.ylabel("Count")
    savefig("ch_incorporation_years")

# --- 4. Last accounts year ---
if "Accounts.LastMadeUpDate" in df_ch.columns:
    df_ch["last_accounts_year"] = df_ch["Accounts.LastMadeUpDate"].dt.year

    plt.figure(figsize=(10, 5))
    df_ch["last_accounts_year"].value_counts().sort_index().plot(kind="bar")
    plt.title("Last Accounts Filed by Year")
    plt.xlabel("Year")
    plt.ylabel("Count")
    savefig("ch_last_accounts_year")

# --- 5. Postcode hotspots ---
postcode_col = "RegAddress.PostCode"
if postcode_col in df_ch.columns:
    plt.figure(figsize=(8, 5))
    df_ch[postcode_col].astype(str).str[:3].value_counts().head(20).plot(kind="bar")
    plt.title("Top 20 Postcode Areas (First 3 Chars)")
    plt.xlabel("Postcode Area")
    plt.ylabel("Number of Companies")
    savefig("ch_top_postcode_areas")


# ==========================
# Common Crawl Dataset Visuals
# ==========================

print("\nGenerating Common Crawl visuals...")

# --- 1. Word counts ---
if "content" in df_crawl.columns:
    df_crawl["word_count"] = df_crawl["content"].fillna("").apply(lambda x: len(str(x).split()))

    plt.figure(figsize=(8, 5))
    sns.histplot(df_crawl["word_count"], bins=50)
    plt.title("Crawl Dataset: Word Count Distribution")
    plt.xlabel("Words per page")
    plt.ylabel("Frequency")
    savefig("crawl_word_count_distribution")

# --- 2. Short page counts ---
if "word_count" in df_crawl.columns:
    df_crawl["short_page"] = df_crawl["word_count"] < 50

    plt.figure(figsize=(6, 4))
    df_crawl["short_page"].value_counts().plot(kind="bar")
    plt.title("Short Pages (<50 words)")
    plt.xticks([0, 1], ["Long", "Short"])
    plt.ylabel("Count")
    savefig("crawl_short_pages")

# --- 3. Domain extensions ---
if "url" in df_crawl.columns:
    df_crawl["domain_ext"] = df_crawl["url"].astype(str).str.extract(r"\.([a-z]{2,5})/?$")

    plt.figure(figsize=(8, 5))
    df_crawl["domain_ext"].value_counts().head(15).plot(kind="bar")
    plt.title("Crawl Dataset: Top Domain Extensions")
    plt.xlabel("Domain")
    plt.ylabel("Count")
    savefig("crawl_domain_extensions")

# --- 4. Parent website distribution ---
if "parent_url" in df_crawl.columns:
    plt.figure(figsize=(10, 6))
    df_crawl["parent_url"].value_counts().head(20).plot(kind="bar")
    plt.title("Top 20 Parent URLs")
    plt.xlabel("Website")
    plt.ylabel("Page Count")
    savefig("crawl_top_parent_urls")


print("\nAll visuals generated and saved to: dataset_visuals/")
