# Data exploration

Purpose
This folder documents the exploratory data analysis (EDA) and dataset investigations that shaped design choices for the entity-matching pipeline linking Companies House records to official company websites.

The goals of the exploration were to:
- Understand Companies House (CH) dataset characteristics (coverage, active vs inactive companies, fields of interest).
- Evaluate potential web data sources (Common Crawl, commercial datasets like FAME) for feasibility.
- Measure practical constraints (scale, noise, crawl coverage) and how they influence choice of blocking and matching strategies.

Files in this folder:
- Companies House EDA
- Common Crawl EDA
- Visuals
- dataset_visuals : contains the visuals outputed from visuals.py

The data for both Companies House and Common Crawl was too large to upload to github. 

The Companies House data can be downloaded from this link: https://download.companieshouse.gov.uk/en_output.html

The Companies House People Of Significant Control data can be downloaded from this link: https://download.companieshouse.gov.uk/en_pscdata.html
