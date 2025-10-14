import csv
import sys
import json
from collections import defaultdict
from scipy import stats
import numpy as np

# Configuration
FILENAME = "202350_filtered_about_contact.csv"
OUTPUT_CSV = "likely_legal.csv"

# Filter thresholds
MIN_LEGAL_DENSITY = 0.01  # 1% of content must be legal terms
MIN_Z_SCORE = 1.5
MAX_P_VALUE = 0.05

# Increase CSV field size limit
csv.field_size_limit(sys.maxsize)

# Legal terms dictionary
LEGAL_TERMS = [
    'barrister', 'barristers',
    'solicitor', 'solicitors',
    'tribunal', 'tribunals',
    'judgement', 'judgements', 'judgment', 'judgments',
    'court', 'courts',
    'legal',
    'law', 'laws',
    'conveyancing',
    'advocate', 'advocates',
    'lawyer', 'lawyers',
    'attorney', 'attorneys',
    'paralegal', 'paralegals',
    'litigation', 'litigations',
    'arbitration', 'arbitrations',
    'mediation', 'mediations',
    'appeal', 'appeals',
    'prosecution', 'prosecutions',
    'defence', 'defences', 'defense', 'defenses',
    'settlement', 'settlements',
    'claim', 'claims',
    'case', 'cases',
    'hearing', 'hearings',
    'verdict', 'verdicts',
    'sentence', 'sentences',
    'injunction', 'injunctions',
    'affidavit', 'affidavits',
    'testimony', 'testimonies',
    'summons', 'summonses',
    'writ', 'writs',
    'subpoena', 'subpoenas',
    'probate',
    'advocacy',
    'jurisdiction', 'jurisdictions',
    'chambers',
]

def count_legal_terms(text):
    """
    Count occurrences of legal terms in text.
    Returns total count and dictionary of individual term counts.
    """
    if not text or not isinstance(text, str):
        return 0, {}
    
    text_lower = text.lower()
    words = text_lower.split()
    
    term_counts = {}
    total_legal_terms = 0
    
    for term in LEGAL_TERMS:
        count = words.count(term)
        if count > 0:
            term_counts[term] = count
            total_legal_terms += count
    
    return total_legal_terms, term_counts

def analyze_legal_sites(filename):
    """
    Identify legal sites using chi-squared test and z-score analysis.
    Groups by parent_url (top-level domains only).
    Filters by: legal_density >= 1% AND passes both statistical tests.
    """
    print(f"Analyzing {filename} for legal sites...")
    print(f"Grouping by parent_url (top-level domains only)")
    print(f"Filters: legal density >= {MIN_LEGAL_DENSITY*100}%, z-score > {MIN_Z_SCORE}, p-value < {MAX_P_VALUE}")
    
    # First pass: aggregate by parent_url
    print("\nPass 1: Aggregating data by parent_url...")
    parent_data = defaultdict(lambda: {
        'legal_count': 0,
        'total_words': 0,
        'term_breakdown': defaultdict(int),
        'postcodes': set(),
        'page_count': 0
    })
    
    row_count = 0
    
    with open(filename, 'r', encoding='utf-8', errors='replace') as f:
        csv_reader = csv.DictReader(f)
        
        for row in csv_reader:
            row_count += 1
            
            parent_url = row['parent_url']
            content = row['content']
            postcodes = row['postcodes']
            
            total_words = len(content.split())
            legal_count, term_breakdown = count_legal_terms(content)
            
            # Aggregate to parent
            parent_data[parent_url]['legal_count'] += legal_count
            parent_data[parent_url]['total_words'] += total_words
            parent_data[parent_url]['page_count'] += 1
            
            for term, count in term_breakdown.items():
                parent_data[parent_url]['term_breakdown'][term] += count
            
            if postcodes:
                parent_data[parent_url]['postcodes'].add(postcodes)
            
            if row_count % 10000 == 0:
                print(f"Processed {row_count:,} rows...", end='\r')
    
    print(f"\nProcessed {row_count:,} total pages")
    print(f"Unique parent domains: {len(parent_data):,}")
    
    # Convert to list format
    all_data = []
    for parent_url, data in parent_data.items():
        all_data.append({
            'parent_url': parent_url,
            'legal_count': data['legal_count'],
            'total_words': data['total_words'],
            'term_breakdown': dict(data['term_breakdown']),
            'unique_legal_terms': len(data['term_breakdown']),
            'postcodes': ', '.join(sorted(data['postcodes'])),
            'page_count': data['page_count']
        })
    
    # Calculate statistics for z-score (using ALL data)
    print("\nPass 2: Calculating statistical metrics...")
    legal_counts = np.array([d['legal_count'] for d in all_data])
    mean_legal = np.mean(legal_counts)
    std_legal = np.std(legal_counts)
    
    print(f"Mean legal terms per domain: {mean_legal:.2f}")
    print(f"Standard deviation: {std_legal:.2f}")
    
    # Calculate corpus-wide proportions for chi-squared
    total_corpus_words = sum(d['total_words'] for d in all_data)
    total_corpus_legal = sum(d['legal_count'] for d in all_data)
    corpus_legal_proportion = total_corpus_legal / total_corpus_words
    
    print(f"Corpus-wide legal term proportion: {corpus_legal_proportion:.4f}")
    
    # Analyze all domains and apply filters
    print("\nPass 3: Computing chi-squared and z-scores, applying filters...")
    results = []
    
    for data in all_data:
        # Calculate legal density
        legal_density = data['legal_count'] / data['total_words'] if data['total_words'] > 0 else 0
        
        # Skip if doesn't meet density threshold
        if legal_density < MIN_LEGAL_DENSITY:
            continue
        
        # Skip if doesn't have enough unique legal terms
        if data['unique_legal_terms'] < 10:
            continue
        
        # Calculate z-score
        z_score = (data['legal_count'] - mean_legal) / std_legal if std_legal > 0 else 0
        
        # Calculate chi-squared test
        observed_legal = data['legal_count']
        observed_nonlegal = data['total_words'] - data['legal_count']
        
        expected_legal = data['total_words'] * corpus_legal_proportion
        expected_nonlegal = data['total_words'] * (1 - corpus_legal_proportion)
        
        if expected_legal > 0 and expected_nonlegal > 0:
            chi2_stat = (
                ((observed_legal - expected_legal) ** 2 / expected_legal) +
                ((observed_nonlegal - expected_nonlegal) ** 2 / expected_nonlegal)
            )
            p_value = 1 - stats.chi2.cdf(chi2_stat, df=1)
        else:
            chi2_stat = 0
            p_value = 1.0
        
        passes_chi_squared = bool(p_value < MAX_P_VALUE)
        passes_z_score = bool(z_score > MIN_Z_SCORE)
        
        # Only keep if passes BOTH tests
        if not (passes_chi_squared and passes_z_score):
            continue
        
        results.append({
            'parent_url': data['parent_url'],
            'postcodes': data['postcodes'],
            'page_count': data['page_count'],
            'legal_term_count': data['legal_count'],
            'total_words': data['total_words'],
            'unique_legal_terms': data['unique_legal_terms'],
            'legal_density': round(legal_density, 4),
            'z_score': round(z_score, 3),
            'chi_squared': round(chi2_stat, 3),
            'p_value': round(p_value, 6),
        })
    
    print(f"Domains passing all filters: {len(results):,}")
    
    if len(results) == 0:
        print("No domains found meeting all filter criteria!")
        return
    
    # Sort by z-score (highest first)
    results.sort(key=lambda x: x['z_score'], reverse=True)
    
    # Save to CSV
    print(f"\nSaving results to {OUTPUT_CSV}...")
    with open(OUTPUT_CSV, 'w', encoding='utf-8', newline='') as f:
        fieldnames = ['parent_url', 'postcodes', 'page_count', 'legal_term_count', 
                     'unique_legal_terms', 'legal_density', 'z_score', 'chi_squared', 
                     'p_value','total_words']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    
    # Print summary statistics
    print(f"\n{'='*80}")
    print("SUMMARY STATISTICS")
    print(f"{'='*80}")
    print(f"Total pages analyzed: {row_count:,}")
    print(f"Unique parent domains: {len(all_data):,}")
    print(f"Domains passing all filters: {len(results):,}")
    print(f"\nTop 10 domains by z-score:")
    for i, result in enumerate(results[:10], 1):
        print(f"  {i:2d}. {result['parent_url']}")
        print(f"      Z-score: {result['z_score']:.2f}, P-value: {result['p_value']:.6f}")
        print(f"      Legal density: {result['legal_density']*100:.2f}%")
        print(f"      Legal terms: {result['legal_term_count']}, Pages: {result['page_count']}")
    
    print(f"\n{'='*80}")
    print("Done!")

# Run the analysis
analyze_legal_sites(FILENAME)