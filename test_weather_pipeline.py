import pytest
import pandas as pd
from sqlalchemy import create_engine
from unittest.mock import Mock, patch
from weather_pipeline import new_engine, connect_to_db, load_data, process_data, calculate_monthly_avg_temp, save_to_sql

# 1. Test new_engine function
@patch('weather_pipeline.create_engine')
def test_new_engine(mock_create_engine):
    # Mock the successful creation of the engine
    mock_create_engine.return_value = Mock()

    engine = new_engine('mssql', 'test_server', 'test_db', integrated_security=True)
    
    # Assert that the engine was created successfully
    mock_create_engine.assert_called_once_with('mssql://test_server/test_db?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server')
    assert engine is not None

# 2. Test connect_to_db function
def test_connect_to_db():
    mock_engine = Mock()
    mock_connection = mock_engine.connect.return_value = Mock()

    connection = connect_to_db(mock_engine)

    # Assert connection is established
    mock_engine.connect.assert_called_once()
    assert connection == mock_connection

# 3. Test load_data function
@patch('weather_pipeline.pd.read_sql')
def test_load_data(mock_read_sql):
    mock_engine = Mock()
    test_query = "SELECT * FROM SeattleWeather"
    
    # Mock returning a sample dataframe
    mock_read_sql.return_value = pd.DataFrame({
        'date': ['2024-01-01', '2024-01-02'],
        'temp_max': [10.0, 12.5],
        'temp_min': [5.0, 7.5]
    })

    data = load_data(mock_engine, test_query)
    
    # Assert read_sql is called and data is returned correctly
    mock_read_sql.assert_called_once_with(test_query, con=mock_engine)
    assert isinstance(data, pd.DataFrame)
    assert data.shape == (2, 3)

# 4. Test process_data function
def test_process_data():
    # Create a sample dataframe
    data = pd.DataFrame({
        'date': ['2024-01-01', 'invalid_date'],
        'temp_max': [10, 'invalid'],
        'temp_min': [5, 7],
        'precipitation': [None, 0.2],
        'wind': ['10.5', None]
    })

    # Process the data
    processed_data = process_data(data)

    # Check if 'date' is correctly converted to datetime with errors coerced
    assert pd.api.types.is_datetime64_any_dtype(processed_data['date'])

    # Check that numerical columns are converted to float and NaN for invalid entries
    assert pd.api.types.is_float_dtype(processed_data['temp_max'])
    assert pd.api.types.is_float_dtype(processed_data['wind'])

    # Check if year and month are extracted
    assert 'year' in processed_data.columns
    assert 'month' in processed_data.columns

# 5. Test calculate_monthly_avg_temp function
def test_calculate_monthly_avg_temp():
    # Create a sample dataframe
    data = pd.DataFrame({
        'year': [2024, 2024],
        'month': [1, 1],
        'temp_max': [10, 15],
        'temp_min': [5, 7]
    })

    # Calculate monthly average temperatures
    monthly_avg_temp = calculate_monthly_avg_temp(data)

    # Check if the aggregation is correct
    assert monthly_avg_temp.shape == (1, 4)
    assert monthly_avg_temp.iloc[0]['avg_temp_max'] == 12.5
    assert monthly_avg_temp.iloc[0]['avg_temp_min'] == 6.0

# 6. Test save_to_sql function
@patch('weather_pipeline.pd.DataFrame.to_sql')
def test_save_to_sql(mock_to_sql):
    mock_engine = Mock()
    table_name = "MonthlyAvgTemperature"

    # Create a sample dataframe
    data = pd.DataFrame({
        'year': [2024],
        'month': [1],
        'avg_temp_max': [12.5],
        'avg_temp_min': [6.0]
    })

    # Call save_to_sql
    save_to_sql(data, mock_engine, table_name)

    # Check if data is saved to the correct SQL table
    mock_to_sql.assert_called_once_with(table_name, con=mock_engine, if_exists='replace', index=False)
