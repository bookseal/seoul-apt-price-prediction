from __future__ import annotations
import pandas as pd

"""
FILE: sampling.py
DESCRIPTION: High-performance Stratified Sampling Pipeline for Seoul Apt Dataset.

OVERALL FLOW & ARCHITECTURE:
    This script transforms a massive raw CSV (1.1M+ rows) into a lightweight,
    representative Parquet sample (100k rows) for high-speed portfolio display.

    [ STEP 1: Ingest ]
          |  read_csv("train.csv")
          v
    [ STEP 2: Translate ] -> rename_columns_to_english()
          |  Convert Korean headers to Global Standard (EN)
          v
    [ STEP 3: Engineering ] -> prepare_sampling_columns()
          |  Extract 'District' and 'Year' to create 'Strata' keys
          v
    [ STEP 4: Sampling ] -> build_stratified_sample()
          |  Perform Proportional Allocation (Layer-by-layer)
          |  Process: [ Population Distribution ] == [ Sample Distribution ]
          v
    [ STEP 5: Export ] -> make_sample_parquet()
          |  Save as Optimized Parquet Format
          v
    [ RESULT ] -> data/sample.parquet (Ready for Streamlit Cloud)

INFOGRAPHIC:
    DATA VOLUME:   [##########] 1.1M Rows (Input)
    SAMPLING:      [==>       ] 100k Rows (Output)
    STRATEGY:      Balanced by (District x Contract Year)
    PERFORMANCE:   Parquet loading is ~10x faster than CSV.

USAGE:
    Run locally: `python src/sampling.py`
    *Keep raw data private; only share the generated sample.*
"""
def rename_columns_to_english(df: pd.DataFrame) -> pd.DataFrame:
	"""
	Rename known Korean column names to English equivalents.

	Process:
	    [한글 컬럼] ----(Mapping)----> [English Column]
	    -----------------------------------------------
	    시군구                    ->  sigungu
	    번지                      ->  jibun
	    본번                      ->  bunji_main
	    부번                      ->  bunji_sub
	    아파트명                  ->  apartment_name
	    전용면적(㎡)              ->  area_m2
	    계약년월                  ->  contract_yyyymm
	    계약일                    ->  contract_day
	    층                        ->  floor
	    건축년도                  ->  built_year
	    도로명                    ->  road_name
	    거래유형                  ->  transaction_type
	    k-단지분류(아파트,주상복합등등)  ->  complex_type
	    k-전체동수                ->  total_buildings
	    k-전체세대수              ->  total_units
	    k-건설사(시공사)          ->  constructor
	    k-시행사                  ->  developer
	    k-연면적                  ->  gross_area_m2
	    k-주거전용면적            ->  residential_area_m2
	    k-관리비부과면적          ->  management_fee_area_m2
	    건축면적                  ->  construction_area_m2
	    주차대수                  ->  parking_spaces
	    좌표X                     ->  coord_x
	    좌표Y                     ->  coord_y
	    target                    ->  price_10k_krw

	Args:
	    df (pd.DataFrame): Raw DataFrame with Korean headers.

	Returns:
	    pd.DataFrame: Cleaned DataFrame with English headers.
	"""
	mapping = {
	    "시군구": "sigungu",
	    "번지": "jibun",
	    "본번": "bunji_main",
	    "부번": "bunji_sub",
	    "아파트명": "apartment_name",
	    "전용면적(㎡)": "area_m2",
	    "계약년월": "contract_yyyymm",
	    "계약일": "contract_day",
	    "층": "floor",
	    "건축년도": "built_year",
	    "도로명": "road_name",
	    "거래유형": "transaction_type",
	    "k-단지분류(아파트,주상복합등등)": "complex_type",
	    "k-전체동수": "total_buildings",
	    "k-전체세대수": "total_units",
	    "k-건설사(시공사)": "constructor",
	    "k-시행사": "developer",
	    "k-연면적": "gross_area_m2",
	    "k-주거전용면적": "residential_area_m2",
	    "k-관리비부과면적": "management_fee_area_m2",
	    "건축면적": "construction_area_m2",
	    "주차대수": "parking_spaces",
	    "좌표X": "coord_x",
	    "좌표Y": "coord_y",
	    "target": "price_10k_krw",
	}
	existing = {k: v for k, v in mapping.items() if k in df.columns}
	return df.rename(columns=existing)

def make_sample_parquet(raw_csv_path: str, out_parquet_path: str, n: int = 100_000) -> None:
	"""
	Orchestrate the creation of a sample Parquet file from raw CSV.

	Pipeline:
	    [ raw_train.csv ]  (Heavy, >500MB)
	           |
	           v
	    ( Rename to EN )
	           |
	           v
	    ( Stratified Sampling )
	           |
	           v
	    [ sample.parquet ] (Light, ~10MB, Fast I/O)

	Note:
	    Run this locally once. Parquet format is used for 
	    high-speed loading on Streamlit Cloud.
	    
	Args:
	    raw_csv_path (str): Path to the original CSV file.
	    out_parquet_path (str): Output destination for the Parquet file.
	    n (int): Number of rows to sample.
	"""
	df = pd.read_csv(raw_csv_path)
	df = rename_columns_to_english(df)
	sample = build_stratified_sample(df, n=n, seed=42)
	sample.to_parquet(out_parquet_path, index=False)

def main() -> None:
	"""
	CLI Entry point for the sampling script.

	Workflow:
	    1. Define paths (Local only).
	    2. Trigger pipeline (CSV -> Parquet).
	    3. Verify completion.

	Safety Alert:
	    !! NEVER commit data/raw/train.csv to Public Git !!
	    !! ONLY commit data/sample.parquet !!
	"""
	raw_train_path = "data/raw/train.csv"
	out_path = "data/sample.parquet"
	make_sample_parquet(raw_train_path, out_path, n=100_000)
	print(f"Saved: {out_path}")

if __name__ == "__main__":
	main()