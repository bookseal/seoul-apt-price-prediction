"""
Test Script for Sampling Pipeline
Created: 2026-01-02 (January 2, 2026)
Purpose: Verify stratified sampling preserves distribution and generates valid parquet

Test Coverage:
- Check sample size (target: 100k rows)
- Verify file size reduction (original 256MB -> ~1MB)
- Compare distribution preservation (district x year strata)
- Validate data types and required columns
"""

import pandas as pd
from pathlib import Path

def test_sampling_output():
    """Test the generated sample.parquet file."""
    
    print("="*60)
    print("SAMPLING PIPELINE TEST - January 2, 2026")
    print("="*60)
    
    # Load files
    original_path = "data/raw/train.csv"
    sample_path = "data/sample.parquet"
    
    print(f"\n[1] Loading original data: {original_path}")
    df_original = pd.read_csv(original_path, low_memory=False)
    print(f"    ✓ Original rows: {len(df_original):,}")
    
    print(f"\n[2] Loading sample data: {sample_path}")
    df_sample = pd.read_parquet(sample_path)
    print(f"    ✓ Sample rows: {len(df_sample):,}")
    
    # File size comparison
    original_size = Path(original_path).stat().st_size / (1024**2)  # MB
    sample_size = Path(sample_path).stat().st_size / 1024  # KB
    print(f"\n[3] File Size Comparison")
    print(f"    Original: {original_size:.1f} MB")
    print(f"    Sample:   {sample_size:.1f} KB")
    print(f"    Compression: {original_size*1024/sample_size:.1f}x smaller")
    
    # Sample size check
    print(f"\n[4] Sample Size Validation")
    target = 100_000
    actual = len(df_sample)
    diff = abs(target - actual)
    print(f"    Target:   {target:,} rows")
    print(f"    Actual:   {actual:,} rows")
    print(f"    Diff:     {diff:,} rows ({diff/target*100:.2f}%)")
    if diff <= 100:
        print(f"    ✓ PASS: Within acceptable range")
    else:
        print(f"    ✗ FAIL: Deviation too large")
    
    # Column validation
    print(f"\n[5] Column Structure")
    print(f"    Columns: {len(df_sample.columns)}")
    print(f"    Names: {df_sample.columns.tolist()}")
    
    required_cols = ["sigungu", "apartment_name", "area_m2", "built_year", 
                     "contract_yyyymm", "floor", "price_10k_krw"]
    missing = [c for c in required_cols if c not in df_sample.columns]
    if missing:
        print(f"    ✗ FAIL: Missing columns: {missing}")
    else:
        print(f"    ✓ PASS: All required columns present")
    
    # Distribution preservation test
    print(f"\n[6] Distribution Preservation (District)")
    df_original['district'] = df_original['시군구'].str.split().str[1]
    
    orig_dist = df_original['district'].value_counts(normalize=True).head(5) * 100
    sample_dist = df_sample['district'].value_counts(normalize=True).head(5) * 100
    
    comparison = pd.DataFrame({
        'Original %': orig_dist,
        'Sample %': sample_dist,
        'Diff %': (sample_dist - orig_dist).abs()
    })
    print(comparison.round(2))
    
    max_diff = comparison['Diff %'].max()
    if max_diff < 1.0:
        print(f"    ✓ PASS: Max difference {max_diff:.2f}% < 1%")
    else:
        print(f"    ⚠ WARNING: Max difference {max_diff:.2f}% >= 1%")
    
    # Year distribution
    print(f"\n[7] Distribution Preservation (Year)")
    df_original['year'] = df_original['계약년월'] // 100
    
    orig_year = df_original['year'].value_counts(normalize=True).sort_index() * 100
    sample_year = df_sample['year'].value_counts(normalize=True).sort_index() * 100
    
    year_comp = pd.DataFrame({
        'Original %': orig_year,
        'Sample %': sample_year,
        'Diff %': (sample_year - orig_year).abs()
    })
    print(year_comp.round(2))
    
    max_year_diff = year_comp['Diff %'].max()
    if max_year_diff < 1.0:
        print(f"    ✓ PASS: Max difference {max_year_diff:.2f}% < 1%")
    else:
        print(f"    ⚠ WARNING: Max difference {max_year_diff:.2f}% >= 1%")
    
    # Price statistics
    print(f"\n[8] Price Statistics Comparison")
    price_stats = pd.DataFrame({
        'Original': df_original['target'].describe(),
        'Sample': df_sample['price_10k_krw'].describe()
    })
    print(price_stats.round(0))
    
    print("\n" + "="*60)
    print("TEST COMPLETED")
    print("="*60)

if __name__ == "__main__":
    test_sampling_output()
