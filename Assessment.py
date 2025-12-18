"""Crime Data pipeline"""

import pandas as pd
import numpy as np
import os

pd.set_option('display.max_columns', None) #Allows me to see all columns within the dataset


#Loading up dataset based on Crime Data from 2020 to present and outputting dataframe to see how the data looks

df = pd.read_csv('Crime_Data_from_2020_to_Present.csv')
print(df)


#Telling the operating system to create a folder called raw if one doesnt exsists already, 
# converting the csv dataset into parquet and specifying what engine is used for conversion

folder = "raw"
if not os.path.exists(folder):
    os.makedirs(folder)
df.to_parquet(f'{folder}/data_raw.parquet', engine='pyarrow')


#Here I am printing the first 20 rows to spot any anomalies within the dataset

df.head(20)


#The following commands starting from the top removes any duplicate records from the dataset, 
# removes values such as "NAN" and "N/A" and replaces them with blank values, changes data fields such as 'Date Rptd' and 'Cd Crm 2' 
# into their correct data type and strips white spaces from string values. 
# Lastly im telling the operating system to create a folder called processed if one doesnt exsists already, 
# converting the csv dataset into parquet and specifying what engine is used for conversion

df = df.drop_duplicates()

df = df.fillna('')

df['Date Rptd'] = pd.to_datetime(df["Date Rptd"])
df['DATE OCC'] = pd.to_datetime(df['DATE OCC'])
df['Premis Cd'] = pd.to_numeric(df['Premis Cd'], errors='coerce') #Coerce is used to force it, and if it cant force it, it will use "NULL" Values
df['Crm Cd 1'] = pd.to_numeric(df['Crm Cd 1'], errors='coerce')
df['Crm Cd 2'] = pd.to_numeric(df['Crm Cd 2'], errors='coerce')
df['Crm Cd 3'] = pd.to_numeric(df['Crm Cd 3'], errors='coerce')
df['Crm Cd 4'] = pd.to_numeric(df['Crm Cd 4'], errors='coerce')
df['Weapon Used Cd'] = df['Weapon Used Cd'].astype(str)

df['AREA NAME'] = df['AREA NAME'].str.strip()
df['Crm Cd Desc'] = df['Crm Cd Desc'].str.strip()
df['Premis Desc'] = df['Premis Desc'].str.strip()
df['Weapon Desc'] = df['Weapon Desc'].str.strip()
df['Status Desc'] = df['Status Desc'].str.strip()
df['LOCATION'] = df['LOCATION'].str.strip()
df['Cross Street'] = df['Cross Street'].str.strip()


folder = "processed"
if not os.path.exists(folder):
    os.makedirs(folder)

df.to_parquet(f'{folder}/data_processed.parquet', engine='pyarrow')

df



#Crime_count purpose is to identify the Top 5 types of criminal activity reported in Los Angeles, 
# while sex_distribution purpose is to identify the distribution of crime victims by biological sex


crime_count = df['Crm Cd Desc'].value_counts().reset_index()
crime_count.columns = ['Type of Crime', 'Total_count']

#print(crime_count.head())

sex_distribution = df['Vict Sex'].dropna().value_counts().reset_index()
sex_distribution.columns = ['victim Sex', 'Incident Count']
sex_distribution['percentage'] = (sex_distribution['Incident Count'] / sex_distribution['Incident Count'].sum()) * 100

#print(sex_distribution)


folder = "analytics"
if not os.path.exists(folder):
    os.makedirs(folder)

crime_count.to_parquet(f'{folder}/data_analytics.parquet', engine='pyarrow')
sex_distribution.to_parquet(f'{folder}/data_analytics.parquet', engine='pyarrow')

df