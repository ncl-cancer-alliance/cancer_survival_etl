import numpy as np
import pandas as pd

from calendar import month_name
from datetime import datetime as dt
from sqlalchemy import create_engine, MetaData, Table, text, insert

from os import listdir
from os.path import isfile, join

import utils.database_util as db
import utils.scrape_util as scrape

#Scrape and download the latest data files from the NHSD site
def scrape_latest_data(
        publication="cancer-survival-in-england",
        target_publications = {
            "index":{"target_ids":["Index"]},
            "cancers-diagnosed":{"target_ids":["adult"]}
        }
        ):
    
    #Get all pages from the publication
    pages = scrape.get_nhsd_pages(publication)

    #Get target_pages
    target_pages = []

    #For each target publication, get the page containing 
    for target in target_publications.keys():
        for page in pages:
            if target in page:
                target_pages.append((target, page))
                break


    #For each page, get the target links
    for publication, page in target_pages:
        #Get all links in the page
        links = scrape.get_file_links_from_page(page)

        #Get the target information
        target_publication = target_publications[publication]

        file_ids = []

        #For each target id, find (exactly 1) target file
        for target_id in target_publication["target_ids"]:
            found_file_ids = []
            for link in links.keys():
                if target_id in link:
                    found_file_ids.append(link)

            if len(found_file_ids) == 1:
                file_ids.append(found_file_ids[0])
            elif len(found_file_ids) == 0:
                print(f"Warning: No files were found for the {publication} publication.")
            else:
                print(f"Warning: Multiple files were found for the {publication} publication. These files won't be processed.")

        #Save all found files to the data directory
        for file_id in file_ids:
            content = scrape.download_file_from_id(links, file_id)

            file_name = file_id + ".xlsx"

            scrape.save_file(content, file_name)

#Upload the survival data
def upload_survival_data(data, table, dsn="SANDPIT", 
                         database="Data_Lab_NCL_Dev", 
                         schema="GrahamR"):
    
    #Establish connection
    engine = db.db_connect(dsn, database)

    with engine.connect() as con:
        #Truncate existing data to prevent overlapping data in the table
        con.execute(text(f"TRUNCATE TABLE [{schema}].[{table}];"))

        #Upload the data
        data.to_sql(table, engine, schema=schema, if_exists="append", 
                    index=False, chunksize=100, method="multi")
        
        con.commit()

#Function to parse the date of the sanpshot from the data file
#This works by checking the first line of the "Methdology" column in the 
#"Notes and definitions" sheet and looking for the Month and Year the represents
#the snapshot of when the data was captured. If the function is unable to do
#this, it throws an error on the assumption it will be caught.
def get_snapshot_date_from_adult(data_file):

    to_skip = 10 #How many lines in the excel before the tabular data begins
    df = pd.read_excel(data_file, sheet_name="Notes and definitions", 
                       skiprows=to_skip)
    
    target_line = df.iloc[0,0]
    month_year = target_line.split(" ")[-3:-1]

    #Check if valid month
    if month_name[0] not in month_name:
        raise Exception()
    
    #Check if valid year
    if int(month_year[1]) < 2000 or int(month_year[1]) > 2100:
        raise Exception() 

    return " ".join(month_year)

#Function for processing the index data
def process_index_data(data_file, target_geographies):

    #Extract data###############################################################

    to_skip = 10    #How many lines in the excel before the tabular data begins
    df_index = pd.read_excel(data_file, sheet_name="Table 5", skiprows=to_skip)

    #Transform data#############################################################

    #NOTE: Some data does not make sense and could be filtered out
    # (i.e 10 years since diagnosis in 2020)

    #Filter to select geographies
    df_index = df_index[df_index["Geography code"].isin(target_geographies)]

    #Derive data_substituion
    df_index["data_substitued"] = np.where(
        df_index["Substituted by Other Geography"].isnull() == 8, 0, 1)

    #Stamp data with timestamp
    df_index["date_uploaded"] = dt.today()

    #Populate extra Persons rows
    df_breast_persons = df_index[(df_index["Cancer site"] == "Breast") 
                                 & (df_index["Gender"] == "Female")].copy()
    df_breast_persons["Gender"] = "Persons"
    df_index = pd.concat([df_index, df_breast_persons])

    #Remove Unused Columns
    columns_to_keep = [
        "Geography name",
        "Geography code",
        "Cancer site", 
        "Gender",
        "Age at diagnosis",
        "Standardisation type",
        "Diagnosis year",
        "Years since diagnosis",
        "Patient numbers",
        "Survival (%)",
        "Lower CI",
        "Upper CI",
        "Precision",
        "Standard error",
        "data_substitued",
        "date_uploaded"]
    
    df_index = df_index[columns_to_keep]

    #Rename columns
    column_map = {
        "Geography name": "Area name",
        "Area code": "Area code",
        "Survival (%)": "sruvival_per"
    }

    df_index = df_index.rename(column_map)

    #Format column names
    df_index.columns = df_index.columns.str.replace('\n', ' ', regex=False)
    df_index.columns = df_index.columns.str.strip().str.replace(' ', '_')
    df_index.columns = df_index.columns.str.lower()

    #Load data##################################################################
    upload_survival_data(df_index, table="cancer_survival_index")

#Function for processing the adult cancer survival (Table 4) data
def process_adult_data_sheet4(data_file, target_geographies):
    
    #Extract data###############################################################

    to_skip = 9 #How many lines in the excel before the tabular data begins
    df_adult4 = pd.read_excel(data_file, sheet_name="Table 4", skiprows=to_skip)

    #Transform data#############################################################

    #NOTE: This process will leave non-age standardised and overall survival
    # data in for non-NCL areas. This needs to be filtered out in the frontend

    #Filter to select geographies
    df_adult4 = df_adult4[df_adult4["Geography code"].isin(target_geographies)]

    #Move the subcategory of the standardisation to its own column
    std = df_adult4["Standardisation type"]
    #    Get all values in the column that are standardised
    df_adult4["standardisation_type_subcategory"] = (
        std.where(std != "Non-standardised"))
    
    #    Remove everything except what is between the brackets
    #    e.g. "Age-standardised (5 age groups)" => "5 age groups"
    std_sc = df_adult4["standardisation_type_subcategory"]
    std_sc = std_sc.str.split("(").str[1]
    std_sc = std_sc.str.split(")").str[0]
    df_adult4["standardisation_type_subcategory"] = std_sc

    #Remove the subcategory from the original standardisation type column
    std = std.str.split("(").str[0]
    std = std.str.strip()
    df_adult4["Standardisation type"] = std

    #Stamp data with timestamp
    df_adult4["date_uploaded"] = dt.today()

    #Stamp data with the window of diagnosis (in the filename)
    diagnosis_window_years = data_file.split(".")[-2].split("_")[-2:]
    df_adult4["date_diagnosis_window"] = "-".join(diagnosis_window_years)

    #Stamp data with the date of the snapshot (if possible)
    try:
        date_snapshot = get_snapshot_date_from_adult(data_file)
    except:
        print("    -> ", 
              "Warning: Unable to extract the snapshot date from the data.")
        date_snapshot = None
        
    df_adult4["date_snapshot"] = date_snapshot

    #List of id columns (not related to the metric value) to keep
    id_cols = [
        "Geography name",
        "Geography code",
        "Cancer site", 
        "Gender",
        "Standardisation type",
        "standardisation_type_subcategory",
        "Years since diagnosis",
        "Patients",
        "date_uploaded",
        "date_diagnosis_window",
        "date_snapshot"]
    
    # List of metric value columns
    value_cols = [
        "Net survival (%)",
        "Overall survival (%)"
    ]
    
    #Remove unused columns
    df_adult4 = df_adult4[id_cols + value_cols]

    #Unpivot the data around the survival metrics
    df_adult4 = pd.melt(df_adult4, 
                        id_vars=id_cols,
                        var_name="survival_metric",
                        value_name="survival_per")
    
    #Format metric names to remove the (%) suffix and capitalise the words
    df_adult4["survival_metric"] = (
        df_adult4["survival_metric"].str.removesuffix(" (%)"))
    
    df_adult4["survival_metric"] = df_adult4["survival_metric"].str.title()

    #Rename columns
    column_map = {
        "Geography name": "Area name",
        "Geography code": "Area code",
        "Patients": "patient_numbers"
    }

    df_adult4 = df_adult4.rename(columns=column_map)

    #Format column names
    df_adult4.columns = df_adult4.columns.str.replace('\n', ' ', regex=False)
    df_adult4.columns = df_adult4.columns.str.strip().str.replace(' ', '_')
    df_adult4.columns = df_adult4.columns.str.lower()

    #Load data##################################################################
    upload_survival_data(df_adult4, table="cancer_survival_adult4")

def main(scrape=True):

    if scrape:
        #Pull the latest data
        print("Downloading the latest data:")
        scrape_latest_data()
        print()

    #Get data files
    data_dir = "./data/"
    data_files = [data_dir + f for f in listdir(data_dir) 
                  if isfile(join(data_dir, f)) 
                  and f.endswith(".xlsx")]
    
    #Set list of target geographies
    # NCL - E54000028, London - E40000003, England - E92000001
    target_geographies = ["E54000028", "E40000003", "E92000001"]

    #Split the files between Index and adult
    print("Processing survival data:")
    for data_file in data_files:
        if data_file.split("/")[-1].startswith("Index"):
            print(f"-> {data_file.split("/")[-1]}")
            process_index_data(data_file, target_geographies)

        if data_file.split("/")[-1].startswith("adult"):
            print(f"-> {data_file.split("/")[-1]}")
            process_adult_data_sheet4(data_file, target_geographies)


main(scrape=False)