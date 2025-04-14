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
                    
scrape_latest_data()