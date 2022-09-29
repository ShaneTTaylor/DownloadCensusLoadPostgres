library(DBI)
library(dplyr)
library(odbc)
library(RCurl)
library(readr)
library(RPostgres)
library(rvest)
library(stringr)

# READ ME: The first four variable assignments must be defined by the user.
# The Census directory will download multiple sets of csv and fixed width flat
# files, which will take up 118 GB.

# Directory for storing the Census Bureau's 2010 Summary File 1
census_parent_directory <- 'D:/Census_2010/'

# User name for the Postgres database in which the Summary Files will be stored
postgres_user <- 'postgres'

# Password for the Postgres database in which the Summary Files will be stored
postgres_password <- ''

# Name of database schema
postgres_db_schema <- 'public'

# 2010 Census Summary File 1 - Technical Documentation
# https://www2.census.gov/programs-surveys/decennial/2010/technical-documentation/complete-tech-docs/summary-file/sf1.pdf

if(!dir.exists(census_parent_directory)) {
    dir.create(census_parent_directory)
}

# Read structure of the parent directory and prepare for download
census_parent_url <- 'https://www2.census.gov/census_2010/04-Summary_File_1/'

census_parent_html <- read_html(census_parent_url)

census_parent_list <- census_parent_html %>%
    html_nodes('td a') %>%
    html_attr('href')

non_region_values <- c('/census_2010/',
                       '0HowToUseMSAccessWithSummaryFile1.pdf',
                       '0README_SF1_v2.doc',
                       '0README_SF1_v2.pdf',
                       'SF1_Access2003.mdb',
                       'SF1_Access2007.accdb',
                       'Urban_Rural_Update/')

region_list <- census_parent_list[!census_parent_list %in% non_region_values]

# Create a child directory for each region, download its zipped files, and unzip
for (i in 1:length(region_list)) {
    census_child_directory <- paste0(census_parent_directory,
                                     str_replace(region_list[i], '/', ''))
    
    if(!dir.exists(census_child_directory)) {
        dir.create(census_child_directory)
    }
    
    if(length(dir(census_child_directory, all.files = TRUE)) == 0) {
        census_child_url <- paste0(census_parent_url, region_list[i])
        
        census_child_html <- read_html(census_child_url)
        
        census_child_list <- census_child_html %>%
            html_nodes('td a') %>%
            html_attr('href')
        
        census_child_zip <- paste0(census_child_url,
                                   str_subset(census_child_list, '.zip'))
        
        temp_file <- tempfile()
        
        download.file(census_child_zip, destfile = temp_file, method = 'curl')
        
        unzip(temp_file, exdir = census_child_directory)
        
        unlink(temp_file)
    }
}

# "How to use Microsoft Access to extract data from the 2010 Census Summary File 1"
# https://www2.census.gov/census_2010/04-Summary_File_1/0HowToUseMSAccessWithSummaryFile1.pdf

# Retrieve import specifications from Microsoft Access database shell to identify 
# fields and data types for each data segment and their geographic headers
access_shell_orig_file <- 'SF1_Access2007.accdb'

access_shell_url <- paste0(census_parent_url, access_shell_orig_file)

access_shell_dest_file <- paste0(census_parent_directory, access_shell_orig_file)

if(!file.exists(access_shell_dest_file)) {
    download.file(access_shell_url, destfile = access_shell_dest_file, method = 'curl')
}

odbc_connect_str <- paste0('Driver={Microsoft Access Driver (*.mdb, *.accdb)};Dbq=',
                           str_replace_all(access_shell_dest_file, '/', '\\\\'), ';')

options(rstudio.connectionObserver.errorsSuppressed = TRUE)

odbc_connect <- dbConnect(odbc(), .connection_string = odbc_connect_str)

# field_descriptors <- dbReadTable(odbc_connect, 'DATA_FIELD_DESCRIPTORS')

imex_columns <- dbReadTable(odbc_connect, 'MSysIMEXColumns')

imex_specs <- dbReadTable(odbc_connect, 'MSysIMEXSpecs')

imex_join <- inner_join(imex_columns, imex_specs, by = 'SpecID')

dbDisconnect(odbc_connect)

options(rstudio.connectionObserver.errorsSuppressed = FALSE)

imex <- imex_join %>%
    mutate(DataType = ifelse(FieldName == 'LOGRECNO', 10, DataType)) %>%
    select(SpecName, SpecType, SpecID, FieldName, DataType, Start, Width) %>%
    arrange(SpecName, Start)

# Define function to add leading zeros to segment numbers
add_leading_zeros <- function(num) {
    if (num < 10) {
        zeros_num <- paste0('0000', num)
    } else {
        zeros_num <- paste0('000', num)
    }
    
    return(zeros_num)
}

# Retrieve all the field names and data types for each of the 47 data segments
segment_fields <- list()

for (i in 1:47) {
    segment_num <- add_leading_zeros(i)
    
    if (i == 45) {
        seg_part_1 <- imex %>%
            filter(str_detect(SpecName, '00045_PT1')) %>%
            select(FieldName, DataType, Start) %>%
            arrange(Start)
        
        overlap_fields <- seg_part_1[1:5, 'FieldName']
        
        seg_part_2 <- imex %>%
            filter(str_detect(SpecName, '00045_PT2')) %>%
            filter(!FieldName %in% overlap_fields) %>%
            select(FieldName, DataType, Start) %>%
            arrange(Start)
        
        segment_fields[[i]] <- rbind(seg_part_1, seg_part_2)
    } else {
        segment_fields[[i]] <- imex %>%
            filter(str_detect(SpecName, segment_num)) %>%
            select(FieldName, DataType, Start) %>%
            arrange(Start)
    }
}

# Retrieve the field names, data types, and column positions for the geographic
# header flat file
geo_header_fields <- imex %>%
    filter(SpecName == 'GEO Header Import Specification') %>%
    mutate(End = Start + Width - 1) %>%
    select(FieldName, DataType, Start, End) %>%
    arrange(Start)

# Define function to convert numeric values for different data types to a
# character string when importing the segment files
data_type <- function(type_num) {
    type_letter <- case_when(type_num == 10 ~ 'c',
                             type_num == 7 ~ 'd',
                             type_num == 4 ~ 'i')
    
    return(type_letter)
}

# Open connection to the Postgres database
postgres_connection <- dbConnect(Postgres(),
                                 user = postgres_user,
                                 password = postgres_password)

# For each region, combine segment data with the appropriate field names and data types
# And combine geographic header data with the appropriate field names and data types
# Then, load data to the Posgres database
for (i in 1:length(region_list)) {
    census_child_directory <- paste0(census_parent_directory,
                                     str_replace(region_list[i], '/', ''),
                                     '/')
    
    region_files <- list.files(census_child_directory)
    
    region_abbrv <- str_sub(region_files[1], 1, 2)
    
    for (j in 1:47) {
        segment_num <- add_leading_zeros(j)
        
        segment <- paste0(region_abbrv, segment_num)
        
        segment_file <- region_files[str_detect(region_files, segment)]
        
        file_name <- paste0(census_child_directory, segment_file)
        
        segment_db_table <- paste0('census_2010_sf1_', segment_num)
        
        if(!dbExistsTable(postgres_connection, name = segment_db_table)) {
            column_names <- tolower(segment_fields[[j]]$FieldName)
            
            column_types <- sapply(segment_fields[[j]]$DataType, data_type)
            
            mod_segments <- c(6, 7, 8, 10, 11, 12, 15, 38)
            
            if (j %in% mod_segments) {
                missing_names <- c('fileid', 'stusab', 'chariter', 'cifsn')
                
                column_names <- c(missing_names, segment_fields[[j]]$FieldName)
                
                missing_types <- c('c', 'c', 'c', 'c')
                
                column_types <- c(missing_types, column_types)
            }
            
            column_types <- paste(column_types, collapse = '')
            
            segment_data <- read_csv(file_name,
                                     col_names = column_names,
                                     col_types = column_types,
                                     n_max = 1000)
            
            dbCreateTable(postgres_connection,
                          name = Id(schema = postgres_db_schema,
                                    table = segment_db_table),
                          fields = head(segment_data, n = 0))
            
            rm(segment_data)
        }
        
        file_name <- str_replace_all(file_name, '/', '\\\\')
        
        sql_statement <- paste0('COPY ', segment_db_table, ' FROM \'', 
                                file_name, '\' DELIMITER \',\' CSV;')
        
        dbBegin(postgres_connection)
        
        dbExecute(postgres_connection, sql_statement)
        
        dbCommit(postgres_connection)
    }
    
    geo_header_file <- region_files[str_detect(region_files, 'geo')]
    
    file_name <- paste0(census_child_directory, geo_header_file)
    
    start_positions <- geo_header_fields$Start
    
    end_positions <- geo_header_fields$End
    
    column_names <- tolower(geo_header_fields$FieldName)
    
    column_types <- sapply(geo_header_fields$DataType, data_type)
    
    column_types <- paste(column_types, collapse = '')
    
    geo_header_data <- read_fwf(file_name,
                                fwf_positions(start = start_positions,
                                              end = end_positions,
                                              col_names = column_names),
                                col_types = column_types,
                                locale = locale(encoding = 'LATIN1'))
    
    geo_header_db_table <- 'census_2010_sf1_geo_header'
    
    if(!dbExistsTable(postgres_connection, name = geo_header_db_table)) {
        dbCreateTable(postgres_connection,
                      name = Id(schema = postgres_db_schema,
                                table = geo_header_db_table),
                      fields = head(geo_header_data, 0))
    }
    
    dbAppendTable(postgres_connection,
                  name = Id(schema = postgres_db_schema,
                            table = geo_header_db_table),
                  value = geo_header_data)
    
    rm(geo_header_data)
}

# Close connection to the Postgres database
dbDisconnect(postgres_connection)
