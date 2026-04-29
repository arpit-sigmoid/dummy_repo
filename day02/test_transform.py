"""
Pytest cases for transform_data function in ETL pipeline
"""

import pytest
import pandas as pd
import numpy as np
from etl_pipeline import transform_data


class TestTransformData:
    """Test suite for transform_data function"""
    
    def test_transform_basic_aggregation(self):
        """Test basic region aggregation with valid data"""
        # Create sample data
        df = pd.DataFrame({
            'order_id': [1, 2, 3, 4],
            'region': ['North', 'North', 'South', 'South'],
            'amount': [100, 200, 150, 250]
        })
        
        result = transform_data(df)
        
        # Verify structure
        assert len(result) == 2
        assert list(result.columns) == ['region', 'total_revenue']
        
        # Verify aggregation
        assert result[result['region'] == 'North']['total_revenue'].values[0] == 300
        assert result[result['region'] == 'South']['total_revenue'].values[0] == 400
    
    
    def test_transform_with_null_amounts(self):
        """Test that null amounts are replaced with 0"""
        df = pd.DataFrame({
            'order_id': [1, 2, 3, 4],
            'region': ['North', 'North', 'South', 'South'],
            'amount': [100, np.nan, 150, np.nan]
        })
        
        result = transform_data(df)
        
        # Verify null values were replaced with 0
        assert result[result['region'] == 'North']['total_revenue'].values[0] == 100
        assert result[result['region'] == 'South']['total_revenue'].values[0] == 150
    
    
    def test_transform_all_null_amounts(self):
        """Test transformation with all null amounts"""
        df = pd.DataFrame({
            'order_id': [1, 2],
            'region': ['North', 'South'],
            'amount': [np.nan, np.nan]
        })
        
        result = transform_data(df)
        
        # All null amounts should become 0
        assert result['total_revenue'].sum() == 0
        assert len(result) == 2
    
    
    def test_transform_single_region(self):
        """Test transformation with single region"""
        df = pd.DataFrame({
            'order_id': [1, 2, 3],
            'region': ['East', 'East', 'East'],
            'amount': [100, 200, 300]
        })
        
        result = transform_data(df)
        
        assert len(result) == 1
        assert result['region'].values[0] == 'East'
        assert result['total_revenue'].values[0] == 600
    
    
    def test_transform_multiple_regions(self):
        """Test transformation with multiple regions"""
        df = pd.DataFrame({
            'order_id': [1, 2, 3, 4, 5, 6],
            'region': ['North', 'South', 'East', 'West', 'North', 'South'],
            'amount': [100, 200, 300, 400, 150, 250]
        })
        
        result = transform_data(df)
        
        assert len(result) == 4
        assert set(result['region']) == {'North', 'South', 'East', 'West'}
        assert result[result['region'] == 'North']['total_revenue'].values[0] == 250
        assert result[result['region'] == 'South']['total_revenue'].values[0] == 450
    
    
    def test_transform_zero_amounts(self):
        """Test transformation with zero amounts"""
        df = pd.DataFrame({
            'order_id': [1, 2, 3],
            'region': ['North', 'North', 'South'],
            'amount': [0, 0, 100]
        })
        
        result = transform_data(df)
        
        assert result[result['region'] == 'North']['total_revenue'].values[0] == 0
        assert result[result['region'] == 'South']['total_revenue'].values[0] == 100
    
    
    def test_transform_large_amounts(self):
        """Test transformation with large amount values"""
        df = pd.DataFrame({
            'order_id': [1, 2],
            'region': ['North', 'South'],
            'amount': [1000000, 2000000]
        })
        
        result = transform_data(df)
        
        assert result[result['region'] == 'North']['total_revenue'].values[0] == 1000000
        assert result[result['region'] == 'South']['total_revenue'].values[0] == 2000000
    
    
    def test_transform_float_amounts(self):
        """Test transformation with decimal amounts"""
        df = pd.DataFrame({
            'order_id': [1, 2, 3],
            'region': ['North', 'North', 'South'],
            'amount': [99.99, 100.01, 50.50]
        })
        
        result = transform_data(df)
        
        north_revenue = result[result['region'] == 'North']['total_revenue'].values[0]
        south_revenue = result[result['region'] == 'South']['total_revenue'].values[0]
        
        assert pytest.approx(north_revenue, 0.01) == 200.00
        assert pytest.approx(south_revenue, 0.01) == 50.50
    
    
    def test_transform_empty_dataframe(self):
        """Test transformation with empty DataFrame"""
        df = pd.DataFrame({
            'order_id': [],
            'region': [],
            'amount': []
        })
        
        result = transform_data(df)
        
        assert len(result) == 0
        assert list(result.columns) == ['region', 'total_revenue']
    
    
    def test_transform_output_structure(self):
        """Test that output has correct structure and data types"""
        df = pd.DataFrame({
            'order_id': [1, 2],
            'region': ['North', 'South'],
            'amount': [100, 200]
        })
        
        result = transform_data(df)
        
        # Verify column names
        assert 'region' in result.columns
        assert 'total_revenue' in result.columns
        assert len(result.columns) == 2
        
        # Verify data types
        assert pd.api.types.is_string_dtype(result['region'])
        assert pd.api.types.is_numeric_dtype(result['total_revenue'])
    
    
    def test_transform_preserves_region_names(self):
        """Test that region names are preserved correctly"""
        df = pd.DataFrame({
            'order_id': [1, 2, 3],
            'region': ['North America', 'South America', 'North America'],
            'amount': [100, 200, 300]
        })
        
        result = transform_data(df)
        
        assert set(result['region']) == {'North America', 'South America'}
        assert result[result['region'] == 'North America']['total_revenue'].values[0] == 400
    
    
    def test_transform_mixed_null_and_values(self):
        """Test transformation with mix of null and non-null amounts"""
        df = pd.DataFrame({
            'order_id': [1, 2, 3, 4, 5],
            'region': ['North', 'North', 'South', 'South', 'East'],
            'amount': [100, np.nan, 200, np.nan, np.nan]
        })
        
        result = transform_data(df)
        
        assert result[result['region'] == 'North']['total_revenue'].values[0] == 100
        assert result[result['region'] == 'South']['total_revenue'].values[0] == 200
        assert result[result['region'] == 'East']['total_revenue'].values[0] == 0
    
    
    def test_transform_case_sensitive_regions(self):
        """Test that region names are case-sensitive"""
        df = pd.DataFrame({
            'order_id': [1, 2, 3],
            'region': ['north', 'North', 'NORTH'],
            'amount': [100, 200, 300]
        })
        
        result = transform_data(df)
        
        # Should have 3 distinct regions (case-sensitive)
        assert len(result) == 3
        assert set(result['region']) == {'north', 'North', 'NORTH'}
    
    
    def test_transform_negative_amounts(self):
        """Test transformation with negative amounts (should be preserved)"""
        df = pd.DataFrame({
            'order_id': [1, 2, 3],
            'region': ['North', 'North', 'South'],
            'amount': [100, -50, 200]
        })
        
        result = transform_data(df)
        
        # Negative amounts should be aggregated normally
        assert result[result['region'] == 'North']['total_revenue'].values[0] == 50
        assert result[result['region'] == 'South']['total_revenue'].values[0] == 200
    
    
    def test_transform_very_large_dataset(self):
        """Test transformation with large dataset"""
        # Create large dataset with 10000 records
        regions = ['North', 'South', 'East', 'West']
        df = pd.DataFrame({
            'order_id': range(10000),
            'region': [regions[i % 4] for i in range(10000)],
            'amount': [100 * (i % 10) for i in range(10000)]
        })
        
        result = transform_data(df)
        
        assert len(result) == 4
        assert set(result['region']) == set(regions)
        # Verify all records were processed
        assert result['total_revenue'].sum() > 0
    
    
    def test_transform_with_special_characters_in_region(self):
        """Test transformation with special characters in region names"""
        df = pd.DataFrame({
            'order_id': [1, 2],
            'region': ['North/South', 'East & West'],
            'amount': [100, 200]
        })
        
        result = transform_data(df)
        
        assert len(result) == 2
        assert 'North/South' in result['region'].values
        assert 'East & West' in result['region'].values
    
    
    def test_transform_returns_dataframe(self):
        """Test that transform_data returns a DataFrame"""
        df = pd.DataFrame({
            'order_id': [1],
            'region': ['North'],
            'amount': [100]
        })
        
        result = transform_data(df)
        
        assert isinstance(result, pd.DataFrame)
    
    
    def test_transform_no_index_reset(self):
        """Test that output DataFrame has reset index"""
        df = pd.DataFrame({
            'order_id': [1, 2, 3],
            'region': ['North', 'North', 'South'],
            'amount': [100, 200, 300]
        })
        
        result = transform_data(df)
        
        # Index should start from 0
        assert result.index[0] == 0
        assert list(result.index) == [0, 1]
