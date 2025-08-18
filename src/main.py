import numpy as np
import pandas as pd

from calendar import month_name
from datetime import datetime as dt
from sqlalchemy import create_engine, MetaData, Table, text, insert

from dotenv import load_dotenv
from os import getenv
from os import listdir
from os.path import isfile, join

from snowflake.connector import connect

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

#Create a Persons gender copy of gender exclusive data
def generalise_gender(df, cancer_site, base_gender):
    #Populate extra Persons rows
    df_gendered = df[(df["Cancer site"] == cancer_site) 
                                 & (df["Gender"] == base_gender)].copy()
    df_gendered["Gender"] = "Persons"
    df = pd.concat([df, df_gendered])

    return df

#Function for processing the index data
def process_index_data(ctx, data_file, target_geographies):

    #Extract data###############################################################

    to_skip = 10    #How many lines in the excel before the tabular data begins
    df_index = pd.read_excel(data_file, sheet_name="Table 5", skiprows=to_skip)

    #Transform data#############################################################

    #NOTE: Some data does not make sense and could be filtered out
    # (i.e 10 years since diagnosis in 2020)

    #Filter to remove sub ICBs
    df_index = df_index[(
        (df_index["Geography type"] == "Cancer Alliance") | 
        (df_index["Geography code"].isin(target_geographies))
    )]

    #Filter to select geographies
    df_index["area_core"] = df_index["Geography code"].isin(target_geographies)

    #Derive data_substituion
    df_index["data_substituted"] = np.where(
        df_index["Substituted by Other Geography"].isnull(), False, True)

    #Stamp data with timestamp
    df_index["date_upload"] = dt.today()

    #Populate extra Persons rows for the breast data
    df_dupe = df_index[(df_index["Cancer site"] == "Breast") 
                       & (df_index["Gender"] == "Female")
                       & (df_index["Age at diagnosis"] == "All ages")].copy()
    df_dupe["Gender"] = "Persons"
    df_index = pd.concat([df_index, df_dupe])

    #Remove the now generalised breast data
    df_index = df_index[~((df_index["Cancer site"] == "Breast") 
                       & (df_index["Gender"] == "Female")
                       & (df_index["Age at diagnosis"] == "All ages"))]

    #Rename the index site to overall for clarity
    df_index["Cancer site"] = (
        df_index["Cancer site"].str.replace('Index', 'Overall'))
    
    #Remove "Other" site from the data
    df_index = df_index[~(df_index["Cancer site"] == "Other")]

    #Remove Unused Columns
    columns_to_keep = [
        "Geography name",
        "Geography code",
        "area_core",
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
        "data_substituted",
        "date_upload"]
    
    df_index = df_index[columns_to_keep]

    #Rename columns
    column_map = {
        "Geography name": "Area name",
        "Geography code": "Area code",
        "Survival (%)": "survival_per"
    }

    df_index = df_index.rename(columns=column_map)

    #Format column names
    df_index.columns = df_index.columns.str.replace('\n', ' ', regex=False)
    df_index.columns = df_index.columns.str.strip().str.replace(' ', '_')
    df_index.columns = df_index.columns.str.lower()

    #Load data##################################################################

    rename_index = {
        "area_code": "AREA_CODE",
        "area_name": "AREA_NAME",
        "area_core": "IS_AREA_CORE",
        "cancer_site": "CANCER_SITE",
        "gender": "GENDER",
        "age_at_diagnosis": "AGE_AT_DIAGNOSIS",
        "standardisation_type": "STANDARDISATION_TYPE",
        "diagnosis_year": "YEAR_OF_DIAGNOSIS",
        "years_since_diagnosis": "YEARS_SINCE_DIAGNOSIS",
        "patient_numbers": "PATIENT_NUMBERS",
        "survival_per": "SURVIVAL_PERCENT",
        "lower_ci": "LOWER_CI",
        "upper_ci": "UPPER_CI",
        "precision": "PRECISION",
        "standard_error": "STANDARD_ERROR",
        "data_substituted": "IS_DATA_SUBTITUTED"
    }

    df_index = df_index[rename_index.keys()].rename(columns=rename_index)

    database = getenv("DATABASE") 
    schema = getenv("SCHEMA")
    destination_table = getenv("DESTINATION_INDEX")
    destination = f"{database}.{schema}.{destination_table}"

    db.upload_df(ctx, df_index, destination, replace=True)

#Function for processing the adult cancer survival (Table 4) data
def process_adult_data_sheet4(ctx, data_file, target_geographies=[]):
    
    #Extract data###############################################################

    to_skip = 9 #How many lines in the excel before the tabular data begins
    df_adult4 = pd.read_excel(data_file, sheet_name="Table 4", skiprows=to_skip)

    #Transform data#############################################################

    #NOTE: This process will leave non-age standardised and overall survival
    # data in for non-NCL areas. This needs to be filtered out in the frontend

    #Filter to mark the core areas (NCL, London, England)
    df_adult4["area_core"] = (
        df_adult4["Geography code"].isin(target_geographies))
    
    #Filter out data that is not core or a Cancer Alliance
    df_adult4 = df_adult4[(
            (df_adult4["area_core"] == True) | 
            (df_adult4["Geography type"] == "Cancer Alliance")
    )]

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
    df_adult4["date_upload"] = dt.today()

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

    #Populate extra Persons rows for breast
    #(This data is only missing for the national figures in the adult data)
    df_breast_persons = df_adult4[(
        (df_adult4["Cancer site"] == "Breast") &
        (df_adult4["Gender"] == "Female") &
        (df_adult4["Geography code"] == "E92000001")
    )].copy()
    df_breast_persons["Gender"] = "Persons"
    df_adult4 = pd.concat([df_adult4, df_breast_persons])

    male_sites = ["Larynx", "Prostate"]
    female_sites = ["Cervix", "Ovary"]

    for male_site in male_sites:
        df_adult4 = generalise_gender(df_adult4, male_site, "Male")

    for female_site in female_sites:
        df_adult4 = generalise_gender(df_adult4, female_site, "Female")

    #List of id columns (not related to the metric value) to keep
    id_cols = [
        "Geography type",
        "Geography name",
        "Geography code",
        "Cancer site", 
        "Gender",
        "Standardisation type",
        "standardisation_type_subcategory",
        "Years since diagnosis",
        "Patients",
        "area_core",
        "date_upload",
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
        "Geography type": "Area type",
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

    rename_adult4 = {
        "area_type": "AREA_TYPE",
        "area_code": "AREA_CODE",
        "area_name": "AREA_NAME",
        "area_core": "IS_AREA_CORE",
        "cancer_site": "CANCER_SITE",
        "gender": "GENDER",
        "standardisation_type": "STANDARDISATION_TYPE",
        "standardisation_type_subcategory": "STANDARDISATION_TYPE_SUBCATEGORY",
        "years_since_diagnosis": "YEARS_SINCE_DIAGNOSIS",
        "patient_numbers": "PATIENT_NUMBERS",
        "survival_metric": "SURVIVAL_METRIC",
        "survival_per": "SURVIVAL_PERCENT",
        "date_diagnosis_window": "DATE_DIAGNOSIS_WINDOW",
        "date_snapshot": "DATE_SNAPSHOT"
    }

    df_adult4 = df_adult4[rename_adult4.keys()].rename(columns=rename_adult4)

    database = getenv("DATABASE") 
    schema = getenv("SCHEMA")
    destination_table = getenv("DESTINATION_ADULT4")
    destination = f"{database}.{schema}.{destination_table}"
    
    db.upload_df(ctx, df_adult4, destination, replace=True)

def main(scrape=True):

    ### Load environment variables 
    load_dotenv(override=True)

    if scrape:
        #Pull the latest data
        print("Downloading the latest data:")
        scrape_latest_data()
        print("-> Download complete\n")

    #Get data files
    data_dir = "./data/"
    data_files = [data_dir + f for f in listdir(data_dir) 
                  if isfile(join(data_dir, f)) 
                  and f.endswith(".xlsx")]
    
    #Set list of target geographies
    # NCL (CA) - E56000027, London - E40000003, England - E92000001
    target_geographies = ["E56000027", "E40000003", "E92000001"]

    #Establish Snowflake connection
    ctx = connect(
        account=getenv("ACCOUNT"),
        user=getenv("USER"),
        authenticator=getenv("AUTHENTICATOR"),
        role=getenv("ROLE"),
        warehouse=getenv("WAREHOUSE"),
        database=getenv("DATABASE"),
        schema=getenv("SCHEMA")
    )

    #Split the files between Index and adult
    print("Processing survival data:")
    for data_file in data_files:
        if data_file.split("/")[-1].startswith("Index"):
            print(f"-> {data_file.split("/")[-1]}")
            process_index_data(ctx, data_file, target_geographies)

        if data_file.split("/")[-1].startswith("adult"):
            print(f"-> {data_file.split("/")[-1]}")
            process_adult_data_sheet4(ctx, data_file, target_geographies)


main(scrape=True)