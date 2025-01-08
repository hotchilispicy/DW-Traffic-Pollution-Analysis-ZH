#!/usr/bin/env python
# coding: utf-8

# ## Imports

# In[1]:


import pandas as pd
import numpy as np
from glob import glob


# In[2]:


# Write custom path here

path_fleet = "fleet/"
path_traffic = "traffic/"
path_rosengarten = "rosengarten/"
path_stampfen = "stampfen/"

# files in directory

files_fleet = glob(path_fleet + "*.csv")
files_traffic = glob(path_traffic + "*.csv")
files_rosengarten = glob(path_rosengarten + "*.csv")
files_stampfen = glob(path_stampfen + "*.csv")

df_fleetcount = pd.read_csv("fleet/car_count.csv")


# In[3]:


# Loading and creating dataframes for traffic folder

df_fleet = {}

# Loop through the files and assign to DataFrames
for file in files_fleet:
    fleet = file.split('/')[-1].split('.')[0]  # Extract the year from the filename
    df_fleet[fleet] = pd.read_csv(file, low_memory=False)


# In[4]:


# Loading and creating dataframes for traffic folder

df_traffic = {}

# Loop through the files and assign to DataFrames
for file in files_traffic:
    year = file.split('_')[-1].split('.')[0]  # Extract the year from the filename
    df_traffic[year] = pd.read_csv(file, low_memory=False)


# In[ ]:


# Loading and creating dataframes for rosengarten folder

df_rosengarten = {}

# Loop through the files and assign to DataFrames
for file in files_rosengarten:
    year = file.split('_')[-1].split('.')[0]  # Extract the year from the filename
    df_rosengarten[year] = pd.read_csv(file, low_memory=False)


# In[ ]:


# Loading and creating dataframes for stampfen folder

df_stampfen = {}

# Loop through the files and assign to DataFrames
for file in files_stampfen:
    year = file.split('_')[-1].split('.')[0]  # Extract the year from the filename
    df_stampfen[year] = pd.read_csv(file, low_memory=False)


# ## Data Quality

# ### fleet

# In[ ]:


for key, df in df_fleet.items():
    print(f"Information for {key}:")
    df.info()
    print("\n" + "-" * 50 + "\n")


# In[ ]:


for key, df in df_fleet.items():
    print(f"Information for {key}:")
    print("Null values per column:")
    print(df.isnull().sum())  # Print the sum of null values for each column
    print("\n" + "-" * 50 + "\n")


# In[ ]:


for key, df in df_fleet.items():
    print(f"Number of duplicate rows in {key}: {df.duplicated().sum()}")


# In[ ]:


# Configurations
columns_to_drop = ["Unnamed: 11", "SET_NAME", "THEMA_NAME", "Unnamed: 11_neu"]
drop_columns_neu = [
    "SUBSET_NAME", "INDIKATOR_ID", "INDIKATOR_NAME", "EINHEIT_KURZ", "EINHEIT_LANG",
    "SUBSET_NAME_neu", "INDIKATOR_ID_neu", "INDIKATOR_NAME_neu", "EINHEIT_KURZ_neu", "EINHEIT_LANG_neu"
]
common_columns = ["BFS_NR", "GEBIET_NAME", "INDIKATOR_JAHR"]
percentage_columns = [
    "elektro %", "elektro new registrations %", "hybrid %", "hybrid new registrations %",
    "other %", "other new registrations %", "benzin %", "benzin new registrations %",
    "diesel %", "diesel new registrations %"
]
column_order = [
    'BFS_NR', 'GEBIET_NAME', 'INDIKATOR_JAHR',
    'benzin', 'benzin %', 'benzin new registrations','benzin new registrations %',
    'elektro', 'elektro %', 'elektro new registrations', 'elektro new registrations %',
    'diesel', 'diesel %', 'diesel new registrations', 'diesel new registrations %',
    'hybrid', 'hybrid %', 'hybrid new registrations', 'hybrid new registrations %',
    'other', 'other %', 'other new registrations', 'other new registrations %',
    'INDIKATOR_VALUE'
]

# Merge DataFrames
merged_fleet = {
    key: (
        df_fleet[key]
        .merge(df_fleet[f"{key}_neu"], on=["BFS_NR", "GEBIET_NAME", "THEMA_NAME", "SET_NAME", "INDIKATOR_JAHR"], suffixes=("", "_neu"))
        .rename(columns={"INDIKATOR_VALUE": f"{key} %", "INDIKATOR_VALUE_neu": f"{key} new registrations %"})
        .drop(columns=drop_columns_neu, errors="ignore")
    )
    for key in df_fleet if not key.endswith("_neu") and f"{key}_neu" in df_fleet
}

# Combine all merged DataFrames
all_merged = None
for df in merged_fleet.values():
    df = df.drop(columns=columns_to_drop, errors="ignore")
    all_merged = df if all_merged is None else all_merged.merge(df, on=common_columns, how="outer")

# Merge with df_fleetcount
if all(col in df_fleetcount.columns for col in common_columns):
    df_fleetcount = df_fleetcount.drop(
        columns=["SET_NAME", "THEMA_NAME", "SUBSET_NAME", "Unnamed: 11", "INDIKATOR_NAME", "EINHEIT_KURZ", "EINHEIT_LANG", "INDIKATOR_ID"], 
        errors="ignore"
    )
    all_merged = (
        all_merged.merge(df_fleetcount, on=common_columns, how="outer")
        .drop(columns=["INDIKATOR_VALUE_x"], errors="ignore")
        .rename(columns={"INDIKATOR_VALUE_y": "INDIKATOR_VALUE"})
    )

# Calculate new columns
for col in percentage_columns:
    all_merged[col.replace(" %", "")] = all_merged["INDIKATOR_VALUE"] * all_merged[col] / 100

# Reorganize columns and save to CSV
if all_merged is not None:
    all_merged = all_merged[column_order]
    all_merged.to_csv("fleet/fleet_result.csv", index=False)
    print("Merged data has been saved to 'fleet_result.csv'.")
else:
    print("No merged data available to save.")


# ### Traffic data

# In[ ]:


# Add 'year' column to each DataFrame based on the key
for year, df in df_traffic.items():
    df['year'] = year  # Assign the key as the 'year' column

# Combine all DataFrames into a single DataFrame
traffic_df = pd.concat(df_traffic.values(), ignore_index=True)

# Combine the years into a single string for each unique grouping (optional)
if 'years' in all_traffic.columns:
    traffic_df['years_merged'] = traffic_df['years'].apply(
        lambda x: ', '.join(map(str, x)) if isinstance(x, list) else str(x)
    )


# In[ ]:


na_summary = pd.DataFrame({
    "missing_values": traffic_df.isna().sum(),   # Count missing values
    "unique_values": traffic_df.nunique()       # Count unique values
})

# Add percentage of missing values
na_summary["missing_percentage"] = (na_summary["missing_values"] / len(traffic_df)) * 100

# Display the summary
print(na_summary)


# In[ ]:


traffic_df = traffic_df.fillna(0)
traffic_df.to_csv('traffic/traffic_df.csv', index=False)


# ### Stampfen strasse

# In[ ]:


stampfen_df = pd.concat(df_stampfen.values(), ignore_index=True)


# In[ ]:


na_summary = pd.DataFrame({
    "missing_values": stampfen_df.isna().sum(),   # Count missing values
    "unique_values": stampfen_df.nunique()       # Count unique values
})

# Add percentage of missing values
na_summary["missing_percentage"] = (na_summary["missing_values"] / len(stampfen_df)) * 100

# Display the summary
print(na_summary)


# In[ ]:


stampfen_df = stampfen_df.fillna(0)
stampfen_df.to_csv('traffic/stampfen_df.csv', index=False)


# In[ ]:


rosengarten_df = pd.concat(df_rosengarten.values(), ignore_index=True)


# In[ ]:


na_summary = pd.DataFrame({
    "missing_values": rosengarten_df.isna().sum(),   # Count missing values
    "unique_values": rosengarten_df.nunique()       # Count unique values
})

# Add percentage of missing values
na_summary["missing_percentage"] = (na_summary["missing_values"] / len(rosengarten_df)) * 100

# Display the summary
print(na_summary)


# In[ ]:


rosengarten_df = rosengarten_df.fillna(0)
rosengarten_df.to_csv('traffic/rosengarten_df.csv', index=False)

