The dataset chosen was "Crime Data from 2020 to Present" which can be found here:https://catalog.data.gov/dataset/crime-data-from-2020-to-present.

Pipeline Flow:

Raw layer: The crime dataset was read in as a CSV file and converted into parquet file and saved to a folder called raw.

Processed layer: Data is cleaned using pandas, fields were converted to their respective data type and this processed data was converted to parquet and saved to a folder called processed.

Analytics layer: The clean data is aggregated to identify the Top 5 types of criminal activity reported in Los Angeles and the distribution of crime victims by biological sex. Results are exported to a folder called analytics.


How to run the pipeline:

Step 1: Ensure the latest version of python is installed on your device
Step 2: Ensure "Crime Data from 2020 to Present" .csv file is in the same directory as the .py file 
Step 3: Install libraries, open your terminal and run
         py -m pip install numpy
	 py -m pip install pandas
	 py -m pip install pyarrow
Step 4: Run the pipeline - Using Python run: 
                           python Assessment.py - for the script
Step 5: verify results by ensuring folders were created with the corresponding .parquet files


Assumptions:
Assumes there is enough storage on device for creating the raw,processed and analytical folders

Assumes the user has the latest version of python and pip installed


Trade-offs

Used pd.set_option('display.max_columns',None) during development for full visibility

