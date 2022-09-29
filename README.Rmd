---
output: 
  html_document: 
    keep_md: yes
---

## Download 2010 Census, Load PostgreSQL Database

Created by Shane Taylor

August 2022

## Summary Description

This project retrieves 2010 Decennial Census data from Summary File 1 and loads these data and their headers in a PostgreSQL database.

The R script `download_census_load_postgres.R` scrapes the Census website for 2010 Decennial Census data. It then downloads zip files for regional and national data, unzips the files, and saves them in the specified directory.

Each file set contains 47 segments of data in CSV files and one geographic header in a fixed-width flat file. This script also downloads an Access database that contains the field names for all of these files and the column widths for the fixed-width flat file.

Finally, the unzipped files are combined with their respective field names and column widths. These data are then copied to the specified PostgreSQL database.
