# Weather-Pipeline
![pandas](https://img.shields.io/badge/pandas-1.6.0-blue) 
![numpy](https://img.shields.io/badge/numpy-1.21.0-green) 
![sqlalchemy](https://img.shields.io/badge/sqlalchemy-1.4.0-red)
![pytest](https://img.shields.io/badge/pytest-7.0.0-yellow)
![logging](https://img.shields.io/badge/logging-built--in-blue)

This project builds a Seattle weather data pipeline that extracts, transforms, and loads weather data using SQL Server and Python. The pipeline extracts weather data from an SQL database, processes it, calculates monthly average temperatures, and updates the SQL tables. Logging is enabled to track the process and handle errors.

The project also includes a separate pytest script for testing and a `.bat` file to automate the pipeline using Windows Task Scheduler.

## Features :

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

## Usage : 

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
   
6. Upon successful execution, the script will:
   - Load data from the SeattleWeather table.
   - Process the data and calculate monthly average temperatures.
   - Save the results into a new table called "MonthlyAvgTemperature".

## Example : 

After running the script, the following monthly average temperature data is stored in SQL and can also be viewed in the console:

| year | month | avg_temp_max | avg_temp_min |
|------|-------|--------------|--------------|
| 2012 | 1     | 45.34        | 36.78        |
| 2012 | 2     | 47.52        | 38.62        |
| 2012 | 3     | 52.61        | 41.95        |
| ...  | ...   | ...          | ...          |

## Error Handling : 

- The script uses a comprehensive logging system to record both successful operations and any exceptions encountered.
- Logs are saved in a file called "weather_pipeline.log" located in the same directory as the script.
- Errors are logged with full traceback information, and the pipeline continues processing without halting on minor issues (e.g., invalid data conversions).
