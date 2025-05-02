# Cancer Survival ETL

ETL code for processing cancer alliance level data from the NHS Digital Cancer Survival Data (https://digital.nhs.uk/data-and-information/publications/statistical/cancer-survival-in-england)

## Changelog

### [1.0.0] - 2025-05-02
#### Added
- Core functionality
- Scrape source data from NHS Digital
- Processes the Cancer Index data and Adult Cancer Survival data

## Usage
The code requires a DSN setup with the NCL Sandpit set up with the name "SANDPIT".

The code is self contained within the src/main.py script.

The docs directory contains scripts for creating the destination tables.

##
*The contents and structure of this template were largely based on the template used by the NCL ICB Analytics team available here: [NCL ICB Project Template](https://github.com/ncl-icb-analytics/ncl_project)*

## Licence
This repository is dual licensed under the [Open Government v3]([https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) & MIT. All code can outputs are subject to Crown Copyright.

## Contact
Jake Kealey - jake.kealey@nhs.net

Project Link: https://github.com/ncl-cancer-alliance/cancer_survival_etl