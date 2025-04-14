import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from io import BytesIO
from zipfile import ZipFile
from datetime import datetime

#To explain the terminalogy in this script:
#The NHSD website is made up of "publications" which contain "pages" 
#which contain "file_links" which contain "files".
#"Publication" is the topic/metric the pages are about
#"Pages" is the period specific page that contains files for that period
#"File Links" are the url links on pages that contain the data files
#"Files" refers to the target data itself

#Get the n most recent pages from the specified nhsd page
def get_nhsd_pages(nhsd_publication,
                   n=False,
                   url="https://digital.nhs.uk", 
                   section="/data-and-information/publications/statistical/"):
    pages = []

    #Get the full url to the publication
    url_full = url + section + nhsd_publication + "/"

    #Make a request to get all pages in the publication
    res = requests.get(url_full)
    res.raise_for_status()
    soup = BeautifulSoup(res.text, 'html.parser')

    #Get the latest page via the HTML div id
    ls_div = soup.find(id="latest-statistics")
    pages.append(ls_div.a.get("href"))

    #If we only want the latest data, we can stop here
    if n == 1:
        return pages
    
    #Get the other pages for previous pages in the publication
    pp_div = soup.find(id="past-publications")
    #Filter on class = "cta__button" to ignore other links included
    pp_pages = pp_div.find_all("a", attrs={"class": "cta__button"})
    
    #If n is specifed, trim to last n results
    if n:
        pp_pages = pp_pages[:n-1]

    #For each result, store the href (can be used to derrive url)
    for page in pp_pages:
        pages.append(page.get("href"))

    return pages

#For a given page, return a list of all files capturing the file id and period
def get_file_links_from_page(page, url="https://digital.nhs.uk"):

    #Make a request to the full url
    full_url = url + page
    res = requests.get(full_url)
    res.raise_for_status()
    soup = BeautifulSoup(res.text, 'html.parser')

    #Split by this div id to isolate the file links
    file_div = soup.find(id="resources")

    #Built a dict of the results so for each file id we have:
    #the url, period (if exists) and file extension
    relevant_files = {}
    for a_tag in file_div.find_all("a"):
        href = a_tag['href']
        #Ignore empty links (Used as comments or messages occasionally)
        if href:
            #Get the filename without the rest of the url
            filename = href.split("/")[-1]
            #Replace unicode figures with spaces and commas
            filename_clean = filename.replace("%20", " ").replace("%2C", ",")

            #Attempt to extract the url, period and file extension
            #Not all files have a period in the name
            try:
                file_id, period_ext = filename_clean.rsplit(",", 1)
                period_ext_arr = period_ext.split(".")
                relevant_files[file_id] = {"url":href, 
                                           "period":period_ext_arr[0],
                                           "ext":period_ext_arr[1]}
            #For files with no period in the name
            except:
                file_id_ext = filename_clean.split(".")
                relevant_files[file_id_ext[0]] = {"url":href, 
                                                  "ext":file_id_ext[1]}
                
    return relevant_files

#Download data files for a given file_id and file_links
def download_file_from_id(file_links, file_id):

    #Make a request for the file
    try:
        target_url = file_links[file_id]["url"]
    except:
        print(f"'{file_id}' could not be found for this publication.")
        return 0
    res = requests.get(target_url)

    #Check if the request was successful
    if res.status_code == 200:
        return res.content
    else:
        print(f"Failed to download file with the following url:\n{target_url}.",
              f"\nStatus code: {res.status_code}")
        return 0

#Save the request content as a file
def save_file(content, file_name,
              dest_dir="./data"):

    #Build the full destination filename including the path
    target_dest = dest_dir + "/" + file_name
    
    #Save the content as a file
    with open(target_dest, "wb") as file:
        file.write(content)