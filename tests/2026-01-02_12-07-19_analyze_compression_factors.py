"""
Analyze Compression Factors in Sampling Pipeline
Created: 2026-01-02 12:07:19 (January 2, 2026 at 12:07 PM)
Purpose: Break down exactly which code decisions led to 262.7x file size reduction

Analysis Coverage:
- Row reduction from stratified sampling (1.1M → 100k)
- Column reduction from required list (52 → 9)
- Format compression from Parquet encoding (CSV → Parquet)
- Total multiplicative effect calculation
"""

import pandas as pd
from pathlib import Path

def analyze_compression():
    """Analyze all factors contributing to file size reduction."""
    
    print("="*70)
    print("COMPRESSION FACTOR ANALYSIS - January 2, 2026 at 12:07 PM")
    print("="*70)
    
    # Load data
    original_path = "data/raw/train.csv"
    sample_path = "data/sample.parquet"
    
    df_original = pd.read_csv(original_path, low_memory=False)
    df_sample = pd.read_parquet(sample_path)
    
    # Factor 1: Row Reduction (Sampling)
    print("\n" + "="*70)
    print("FACTOR 1: ROW REDUCTION (Stratified Sampling)")
    print("="*70)
    print(f"Original rows: {len(df_original):,}")
    print(f"Sample rows:   {len(df_sample):,}")
    row_factor = len(df_original) / len(df_sample)
    print(f"\n→ Reduction Factor: {row_factor:.2f}x")
    print(f"\nCode responsible:")
    print(f"  build_stratified_sample(df, n=100_000, seed=42)")
    print(f"  └─ n parameter controls sample size")
    
    # Factor 2: Column Reduction
    print("\n" + "="*70)
    print("FACTOR 2: COLUMN REDUCTION (Required List)")
    print("="*70)
    print(f"Original columns: {len(df_original.columns)}")
    print(f"Sample columns:   {len(df_sample.columns)}")
    col_factor = len(df_original.columns) / len(df_sample.columns)
    print(f"\n→ Reduction Factor: {col_factor:.2f}x")
    
    print(f"\nOriginal columns ({len(df_original.columns)}):")
    for i, col in enumerate(df_original.columns, 1):
        print(f"  {i:2d}. {col}")
    
    print(f"\nSample columns ({len(df_sample.columns)}):")
    for i, col in enumerate(df_sample.columns, 1):
        print(f"  {i}. {col}")
    
    print(f"\nCode responsible:")
    print(f"  required = [")
    print(f"      'sigungu', 'apartment_name', 'area_m2',")
    print(f"      'built_year', 'contract_yyyymm', 'floor', 'price_10k_krw'")
    print(f"  ]")
    print(f"  base = df[[c for c in required if c in df.columns]].copy()")
    
    # Factor 3: Format Compression
    print("\n" + "="*70)
    print("FACTOR 3: FORMAT COMPRESSION (CSV → Parquet)")
    print("="*70)
    
    # Create temporary CSV with same data for comparison
    temp_csv_path = 'data/temp_sample_for_comparison.csv'
    df_sample.to_csv(temp_csv_path, index=False)
    
    csv_size_kb = Path(temp_csv_path).stat().st_size / 1024
    parquet_size_kb = Path(sample_path).stat().st_size / 1024
    format_factor = csv_size_kb / parquet_size_kb
    
    print(f"Same data (99,995 rows × 9 columns):")
    print(f"  CSV format:     {csv_size_kb:,.1f} KB")
    print(f"  Parquet format: {parquet_size_kb:,.1f} KB")
    print(f"\n→ Compression Factor: {format_factor:.2f}x")
    
    print(f"\nCode responsible:")
    print(f"  sample.to_parquet(out_parquet_path, index=False)")
    print(f"  └─ Parquet uses columnar storage + compression")
    
    # Clean up temp file
    Path(temp_csv_path).unlink()
    
    # Total Effect
    print("\n" + "="*70)
    print("TOTAL COMPRESSION EFFECT")
    print("="*70)
    
    original_size_mb = Path(original_path).stat().st_size / (1024**2)
    sample_size_kb = Path(sample_path).stat().st_size / 1024
    total_factor = (original_size_mb * 1024) / sample_size_kb
    
    print(f"\nOriginal file:  {original_size_mb:,.1f} MB (train.csv)")
    print(f"Sample file:    {sample_size_kb:,.1f} KB (sample.parquet)")
    print(f"\n→ Total Reduction: {total_factor:.1f}x")
    
    print(f"\nMultiplicative breakdown:")
    print(f"  {row_factor:.2f} (rows) × {col_factor:.2f} (cols) × {format_factor:.2f} (format)")
    calculated = row_factor * col_factor * format_factor
    print(f"  = {calculated:.1f}x (calculated)")
    print(f"  ≈ {total_factor:.1f}x (actual)")
    
    # Summary
    print("\n" + "="*70)
    print("SUMMARY: CODE DECISIONS → SIZE REDUCTION")
    print("="*70)
    print("""
1. Choosing n=100,000 in build_stratified_sample()
   → Kept only 8.9% of rows
   → {:.1f}x reduction

2. Selecting 7 required columns instead of all 52
   → Kept only 13.5% of columns  
   → {:.1f}x reduction

3. Using .to_parquet() instead of .to_csv()
   → Columnar compression + encoding
   → {:.1f}x reduction

TOTAL: 244 MB → 951 KB ({:.1f}x smaller)
    """.format(row_factor, col_factor, format_factor, total_factor))
    
    print("="*70)
    print("ANALYSIS COMPLETED")
    print("="*70)

if __name__ == "__main__":
    analyze_compression()
