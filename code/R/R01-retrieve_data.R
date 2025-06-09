# Load packages ----
library(tidyverse)
library(bit64)

# Create directory for processed data and diagnostics ----
dir.create(here::here("data", "databricks"), showWarnings = FALSE, recursive=TRUE)

# Connect to database ----
con = DBI::dbConnect(
  odbc::odbc(),
  dsn = 'databricks',
  timeout = 60,
  HTTPPath = 'sql/protocolv1/o/847064027862604/0622-162121-dts9kxvy',
  PWD = rstudioapi::askForPassword("Password please:")
)


# Specify database and list of tables to extract ----
database = "dsa_391419_j3w9t_collab"
table_list = list(
  "registry_ssnap__cohort_final",
  "registry_ssnap__cohort_primary_care_meds_first_code_ps"
) 

# Load, prep and save each table
table_list %>% 
  walk(function(.x){
    
    # Load table
    table = DBI::dbGetQuery(con, paste0("SELECT * FROM ", database, ".", .x))
    
    # Convert integer64 class to integer
    table = table %>%
      mutate(across(row_id, as.character)) %>% 
      mutate(across(where(bit64::is.integer64), as.integer))
    
    # Save table as .rds
    write_rds(table, file = here::here("data", "databricks", paste0(.x, ".rds")))
    
  })
