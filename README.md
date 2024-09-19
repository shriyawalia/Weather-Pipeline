# Weather-Pipeline

This project builds a Seattle weather data pipeline that extracts, transforms, and loads weather data using SQL Server and Python. The pipeline extracts weather data from an SQL database, processes it, calculates monthly average temperatures, and updates the SQL tables. Logging is enabled to track the process and handle errors.

The project also includes a separate pytest script for testing and a `.bat` file to automate the pipeline using Windows Task Scheduler.

## Features

1. **Database Integration:**
   - Connects to SQL Server using SQLAlchemy to load weather data.
   - Performs SQL queries and saves processed data back to the database.

2. **Data Processing:**
   - Converts date columns to a proper datetime format.
   - Converts numerical columns to float type, handling invalid values.
   - Extracts year and month from dates.
   - Handles missing values and invalid conversions gracefully.

3. **Monthly Temperature Averages:**
   - Calculates the monthly average of maximum and minimum temperatures.
   - Saves the results in a new SQL table.

4. **Logging:**
   - Logs key events such as data loading, processing, and error occurrences.
   - Tracks issues and logs error details without halting the pipeline.

5. **Automated Testing:**
   - Includes a pytest script to run tests on the functions in the pipeline, ensuring code reliability and correctness before production.

6. **Task Scheduling:**
   - Uses a `.bat` file for automation with Windows Task Scheduler, allowing the pipeline to run periodically without manual intervention.

## Usage

1. Open the script `weather_pipeline.py` and adjust the following database parameters in the `main()` function:
   - **server**: Replace with your SQL Server name.
   - **database**: Replace with the appropriate database name.
2. Ensure that the `SeattleWeather` table exists in your SQL Server and contains the necessary weather data.
3. Run the automated test: 
   ```bash
   pytest test_weather_pipeline.py
4. Run the pipeline manually:
   ```bash
   python weather_pipeline.py
5. Alternatively, you can run the pipeline using the Task Scheduler setup via the .bat file.
6. **Upon successful execution, the script will:**
   -Load data from the SeattleWeather table.
   -Process the data and calculate monthly average temperatures.
   -Save the results into a new table called "MonthlyAvgTemperature".

