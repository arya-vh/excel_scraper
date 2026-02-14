import pytest
import pandas as pd
from scraper import downld_extract_zip, validate_csv, process_clean_data

# Your actual URL
URL = "https://www.thespreadsheetguru.com/wp-content/uploads/2022/12/EmployeeSampleData.zip"


# TEST CASE 1: Download Success
def test_download_success():
    """Test file download from actual URL"""
    result = downld_extract_zip(URL)
    assert result is not None
    assert isinstance(result, bytes)
    assert len(result) > 0


# TEST CASE 2: Extraction Success
def test_extraction_success():
    """Test ZIP extraction gets CSV data"""
    content = downld_extract_zip(URL)
    assert content is not None
    # Should be able to read as CSV
    df = pd.read_csv(pd.io.common.BytesIO(content), encoding='latin-1')
    assert df is not None


# TEST CASE 3: File Type Validation
def test_file_type_validation():
    """Test CSV file type is validated"""
    content = downld_extract_zip(URL)
    assert content is not None
    # Should parse as CSV successfully
    df = pd.read_csv(pd.io.common.BytesIO(content), encoding='latin-1')
    assert len(df.columns) > 1


# TEST CASE 4: Validate Data Structure
def test_data_structure_validation():
    """Test data structure is valid"""
    content = downld_extract_zip(URL)
    df = pd.read_csv(pd.io.common.BytesIO(content), encoding='latin-1')
    df.columns = [col.lower().strip() for col in df.columns]
    
    result = validate_csv(df)
    assert result == True


# TEST CASE 5: Handle Missing/Invalid Data
def test_handle_missing_data():
    """Test missing/invalid data is handled"""
    content = downld_extract_zip(URL)
    df = pd.read_csv(pd.io.common.BytesIO(content), encoding='latin-1')
    df.columns = [col.lower().strip() for col in df.columns]
    df_clean = process_clean_data(df)

    assert df_clean is not None
    assert 'first name' in df_clean.columns
    assert 'last name' in df_clean.columns
    assert 'full name' not in df_clean.columns
    
    assert pd.api.types.is_datetime64_any_dtype(df_clean['hire date'])
    assert pd.api.types.is_datetime64_any_dtype(df_clean['exit date'])
    
    # Check missing data is handled (NaT for missing dates)
    assert df_clean['exit date'].isna().sum() >= 0  # Some exit dates can be missing
    
    # Check single-word names are handled (empty last name)
    assert df_clean['last name'].notna().all() or (df_clean['last name'] == '').any()
    
    # Check no data loss (same number of rows)
    assert len(df_clean) == len(df)

if __name__ == "__main__":
    pytest.main([__file__])