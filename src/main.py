import numpy as np
import pandas as pd

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

#Function for processing the index data
def process_index_data(data_file, target_geographies):

    #Extract data###############################################################

    df_index = pd.read_excel(data_file, sheet_name="Table 5", skiprows=10)

    #Transform data#############################################################

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

    print(df_index.shape)

    #Load data##################################################################
    upload_survival_data(df_index, table="cancer_survival_index")


#Function for processing the adult cancer survival (Table 4) data
def process_adult_data_sheet4(data_file, target_geographies):
    pass

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