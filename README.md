# parliamentarian-data-scraping
Political Representation: Parliamentary Data Extraction (France & Brazil)

Project Overview
This project provides a comprehensive automated pipeline to extract and structure demographic and professional data for current and former parliamentarians from the official websites of the French National Assembly and Senate, and the Brazilian Chamber of Deputies and Federal Senate.

The goal was to construct a large-scale dataset for empirical research on political behavior, focusing on the human capital characteristics of legislators.

Key Features
Multi-Source Web Scraping: Developed robust scrapers using Python (BeautifulSoup and Selenium) to navigate and extract data from multiple institutional web architectures.
Complex Data Extraction: Utilized Regular Expressions (Regex) to parse unstructured text and extract specific attributes such as age, gender, educational background, and tenure length.
Structured Dataset Construction: Cleaned and merged raw data into a relational format using Pandas, ensuring consistency across different national legislative systems.
Version Control: Managed all scripts and data schemas using GitHub to ensure reproducibility and collaborative transparency.

Data Description
The extracted dataset includes the following variables for both chambers of the French and Brazilian parliaments:

Personal Info: Full name, age/date of birth, gender, and highest educational degree attained.
Professional Info: Years of service, party affiliations (where available), committee memberships, and previous occupations.

Technical Stack
Language: Python
Libraries: BeautifulSoup, Selenium, re (Regular Expressions), Pandas, Requests
Workflow: GitHub (Version Control)
