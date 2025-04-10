# NCL Cancer Alliance Project Template

This git repository contains a shell that should be used as the default structure for new projects
in the analytical team.  It won't fit all circumstances perfectly, and you can make changes and issue a 
pull request for new features / changes.

The aim of this template is two-fold: firstly to give a common structure for analytical projects to aid
reproducibility, secondly to allow for additional security settings as default to prevent accidental upload of files that should not be committed to Git and GitHub.

__Please update/replace this README file with one relevant to your project__

## To use this template, please use the following practises:

* Put any data files in the `data` folder.  This folder is explicitly named in the .gitignore file.  A further layer of security is that all xls, xlsx, csv and pdf files are also explicit ignored in the whole folder as well.  ___If you need to commit one of these files, you must use the `-f` (force) command in `commit`, but you must be sure there is no identifiable data.__
* Save any documentation in the `docs` file.  This does not mean you should avoid commenting your code, but if you have an operating procedure or supporting documents, add them to this folder.
* Please save all output: data, formatted tables, graphs etc. in the output folder.  This is also implicitly ignored by git, but you can use the `-f` (force) command in `commit` to add any you wish to publish to github.


### Please also consider the following:
* Linting your code.  This is a formatting process that follows a rule set.  We broadly encourage the tidyverse standard, and recommend the `lintr` package.
* Comment your code to make sure others can follow.
* Consider your naming conventions: we recommend `snake case` where spaces are replaced by underscores and no capitals are use. E.g. `outpatient_referral_data`


This repository is dual licensed under the [Open Government v3]([https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) & MIT. All code can outputs are subject to Crown Copyright.

## Scripting Guidance

Please refer to the NCL ICB Analytics Team Scripting Onboarding documentation for instructions on setting up coding projects including virtual environments (venv).

The Scripting Onboarding documentation is available here: [Scripting Onboarding](https://nhs.sharepoint.com/:f:/r/sites/msteams_3c6e53/Shared%20Documents/Data%20Science?csf=1&web=1&e=ArWnMM)

## Changelog

### [1.0.0] - 2025-04-08
#### Added
- Initial release of the project template

*The contents and structure of this template were largely based on the template used by the NCL ICB Analytics team available here: [NCL ICB Project Template](https://github.com/ncl-icb-analytics/ncl_project)*