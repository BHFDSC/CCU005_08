library(finalfit)
library(tidyverse)
library(lubridate)

cohort = read_rds(here::here("data", "databricks", "registry_ssnap__cohort_final.rds"))

# Stroke Fatality -------
cohort = cohort %>% 
  mutate(
    # Death within 30 days of index stroke 
    stroke_fatality = case_when(
      date_of_death <= (index_stroke_date + days(30)) ~ "Fatal",
      TRUE ~ "Non-fatal"
    ) %>% 
      factor()
  )

# Year of stroke ------
cohort = cohort %>% 
  mutate(
    year_of_stroke = year(index_stroke_date) %>%
      factor() %>% 
      ff_label("Year of stroke")
  )

# Covariate flags to factor ----
col_covariate_flags = c(
  "angina", "arrhythmia", "atrial_fibrillation", "cancer",
  "chronic_kidney_disease", "copd", "deep_vein_thrombosis", "dementia",
  "depression", "diabetes", "hypercholesterolaemia",
  "hypertension", "liver_disease", "myocardial_infarction", "obesity",
  "prior_anticoagulants_flag",
  "prior_antihypertensives_flag",
  "prior_antiplatelets_flag",
  "prior_lipid_lowering_drug_flag"
)

cohort = cohort %>% 
  mutate(across(all_of(col_covariate_flags), function(.x){
    .x %>% 
      factor() %>% 
      fct_na_value_to_level("0") %>% 
      fct_recode("Yes" = "1", "No" = "0") %>% 
      fct_relevel("No")
  }))

# Add labels and refactoring ----

cohort = cohort %>% 
  mutate(
    
    ## Age ---------
    
    age_on_stroke_date = age_on_stroke_date %>% 
      ff_label("Age (years)"),
    
    ## Age group ------
    
    age_group = age_on_stroke_date %>% 
      cut(
        breaks = c(18, 60, 70, 80, 90, Inf),
        labels = c("18-59", "60-69", "70-79", "80-89", "90+"),
        include.lowest = TRUE, right = FALSE
      ) %>% 
      ff_label("Age group (years)"),
    
    ## Sex ------
    
    sex = sex %>% 
      factor() %>%
      fct_recode(
        "Male" = "M",
        "Female" = "F"
      ) %>% 
      fct_relevel("Female") %>% 
      ff_label("Sex"),
    
    ## Ethnicity ---------
    
    ethnicity = ethnicity_5_group %>% 
      factor() %>% 
      fct_recode(
        "Asian" = "Asian or Asian British",
        "Black" = "Black, Black British, Caribbean or African",
        "Mixed" = "Mixed or multiple ethnic groups",
        "Other" = "Other ethnic group"
      ) %>% 
      fct_na_value_to_level("Unknown") %>% 
      fct_relevel("White") %>% 
      fct_relevel("Unknown", after = Inf) %>% 
      ff_label("Ethnicity"),
    

    ## Region ------
    
    region = region %>% 
      factor() %>%
      ff_label("Region"),
    
    
    ## IMD ------
    
    imd_19_quintile = imd_19_quintile %>% 
      factor() %>%
      ff_label("IMD 2019 quintiles"),
    
    
    ## Smoking status ------
    
    smoking_status_date = pmax(
      smoking_status_current_date, smoking_status_ex_date,
      smoking_status_never_date, na.rm = TRUE
    ),

    smoking_status = case_when(
      smoking_status_date == smoking_status_current_date ~ "Current smoker",
      smoking_status_date == smoking_status_ex_date ~ "Ex-smoker",
      smoking_status_date == smoking_status_never_date ~ "Never smoked"
    ) %>%
      factor() %>%
      fct_na_value_to_level("Unknown") %>%
      fct_relevel("Current smoker", "Ex-smoker", "Never smoked") %>%
      ff_label("Smoking status"),

    
    ## Body mass index (BMI) -----
    
    body_mass_index_value = body_mass_index_value %>% 
      ff_label("Body mass index (BMI)"),
    
    ## eGFR  -----
    
    egfr_value = egfr_value %>% 
      ff_label("eGFR"),
    
    ## Glycated haemoglobin ----
    
    glycated_haemoglobin_value = glycated_haemoglobin_value %>% 
      ff_label("Glycated haemoglobin"),
    
    
    ## HDL cholesterol ----
    
    hdl_cholesterol_value = hdl_cholesterol_value %>% 
      ff_label("HDL cholesterol"),
    
    
    ## Total cholesterol ----
    
    total_cholesterol_value = total_cholesterol_value %>% 
      ff_label("Total cholesterol"),
    
    
    ## Systolic blood pressure ----
    
    systolic_blood_pressure_value = systolic_blood_pressure_value %>% 
      ff_label("Systolic blood pressure"),
    
    
    ## Angina ----
    
    angina = angina %>% 
      ff_label("Angina"),
    
    ## Arrhythmia ----
    
    arrhythmia = arrhythmia %>% 
      ff_label("Arrhythmia"),
    
    ## Atrial Fibrillation ----
    
    atrial_fibrillation = atrial_fibrillation %>% 
      ff_label("Atrial fibrillation"),
    
    ## Myocardial Infarction ----
    
    myocardial_infarction = myocardial_infarction %>% 
      ff_label("Myocardial infarction"),
    
    ## Atrial Fibrillation or Myocardial Infarction ----
    
    atrial_fibrillation_or_myocardial_infarction = case_when(
      (atrial_fibrillation == "Yes") |
        (myocardial_infarction == "Yes") ~ "Yes",
      TRUE ~ "No"
    ) %>% 
      factor() %>% 
      ff_label("Atrial fibrillation or myocardial infarction"),
    
    
    ## Diabetes ----
    
    diabetes = diabetes %>% 
      ff_label("Diabetes"),
    
    ## Hypercholesterolaemia ----
    
    hypercholesterolaemia = hypercholesterolaemia %>% 
      ff_label("Hypercholesterolaemia"),
    
    ## Hypertension ----
    
    hypertension = hypertension %>% 
      ff_label("Hypertension"),
    
    ## Obesity ----
    
    obesity = obesity %>% 
      ff_label("Obesity"),
    
    ## Cancer ----
    
    cancer = cancer %>% 
      ff_label("Cancer"),
    
    ## Chronic kidney disease ----
    
    chronic_kidney_disease = chronic_kidney_disease %>% 
      ff_label("Chronic kidney disease"),
    
    ## Liver disease ----
    
    liver_disease = liver_disease %>% 
      ff_label("Liver disease"),
    
    ## COPD ----
    
    copd = copd %>% 
      ff_label("Chronic obstructive pulmonary disease"),
    
    ## Deep vein thrombosis ----
    
    deep_vein_thrombosis = deep_vein_thrombosis %>% 
      ff_label("Deep vein thrombosis"),
    
    ## Dementia ----
    
    dementia = dementia %>% 
      ff_label("Dementia"),
    
    ## Depression ----
    
    depression = depression %>% 
      ff_label("Depression"),
    
    ## Pre-stroke anticoagulants ----
    prior_anticoagulants_flag = prior_anticoagulants_flag %>% 
      ff_label("Pre-stroke anticoagulants"),
    
    ## Pre-stroke antihypertensives ----
    prior_antihypertensives_flag = prior_antihypertensives_flag %>% 
      ff_label("Pre-stroke antihypertensives"),
    
    ## Pre-stroke antiplatelets ----
    prior_antiplatelets_flag = prior_antiplatelets_flag %>% 
      ff_label("Pre-stroke antiplatelets"),
    
    ## Pre-stroke statins ----
    prior_lipid_lowering_drug_flag = prior_lipid_lowering_drug_flag %>% 
      ff_label("Pre-stroke lipid lowering drugs"),
    
    ## Length of stay
    length_of_stay = length_of_stay %>% 
      ff_label("Length of stay (days)"),
    
    ## Home-time
    home_time = home_time %>% 
      ff_label("Home-time"),
    
    ## Covid infection 14 days prior
    covid_infection_14_days = case_when(
      (covid_positive_date >= (index_stroke_date - days(14)))
      & (covid_positive_date <= index_stroke_date) ~ "Yes",
      TRUE ~ "No"
    ) %>% 
      factor() %>% 
      ff_label("COVID-19 within 14 days prior to stroke"),
    
    ## Covid infection ever
    covid_infection_prior = case_when(
      covid_positive_date <= index_stroke_date ~ "Yes",
      TRUE ~ "No"
    ) %>% 
      factor() %>% 
      ff_label("History of COVID-19"),
    
    ## Charlson comorbidity index
    cci = (
      if_else(cci_ami == 1, true = 1, false = 0, missing = 0)
      + if_else(cci_chf == 1, true = 1, false = 0, missing = 0)
      + if_else(cci_pvd == 1, true = 1, false = 0, missing = 0)
      + if_else(cci_cevd == 1, true = 1, false = 0, missing = 0)
      + if_else(cci_dementia == 1, true = 1, false = 0, missing = 0)
      + if_else(cci_cpd == 1, true = 1, false = 0, missing = 0)
      + if_else(cci_rheumd == 1, true = 1, false = 0, missing = 0)
      + if_else(cci_hp == 1,  true = 2, false = 0, missing = 0)
      + if_else(cci_rend == 1,  true = 2, false = 0, missing = 0)
      + if_else(cci_aids == 1,  true = 6, false = 0, missing = 0)
      + case_when(
        cci_diabwc == 1 ~ 2,
        cci_diab == 1 ~ 1,
        TRUE ~ 0
      )
      + case_when(
        cci_metacanc == 1 ~ 6,
        cci_canc == 1 ~ 2,
        TRUE ~ 0
      )
      + case_when(
        cci_msld == 1 ~ 3,
        cci_mld == 1 ~ 1,
        TRUE ~ 0
      )
    ) %>% 
      ff_label("Charlson comorbidity index"),
    
    ## Charlson comorbidity index (factor)
    cci_fct = case_when(
      cci == 0 ~ "0",
      cci == 1 | cci == 2 ~ "1-2",
      cci == 3 | cci == 4 ~ "3-4",
      cci > 4 ~ "5+"
    )%>%
      factor() %>% 
      fct_relevel("0", "1-2", "3-4", "5+") %>% 
      ff_label("Charlson comorbidity index "),
    
    ## NIHSS arrival
    nihss_arrival_fct = nihss_arrival %>% 
      cut(
        breaks = c(0, 4, 10, 15, 21, Inf),
        labels = c("0-4", "5-10", "11-15", "16-21", "22+"),
        include.lowest = TRUE
      ) %>%
      factor() %>%
      fct_relevel("0-4", "5-10", "11-15", "16-21", "22+") %>%
      fct_na_value_to_level(level = "(missing)") %>% 
      ff_label("NIHSS score on arrival"),
    
    ## Rankin before stroke
    rankin_before_stroke_fct = rankin_before_stroke %>% 
      factor() %>% 
      fct_na_value_to_level(level = "(missing)") %>% 
      ff_label("Rankin score before stroke"),
    
    ## Rankin at discharge
    rankin_at_discharge_fct = rankin_discharge %>% 
      factor() %>% 
      fct_na_value_to_level(level = "(missing)") %>% 
      ff_label("Rankin score at discharge"),
    
    # Harmonised stroke type -----
    stroke_type_harmonised = stroke_type_harmonised %>% 
      factor() %>% 
      fct_relevel("Ischaemic", "Haemorrhagic", "Unknown") %>% 
      ff_label("Stroke type"),
    
    # Data source combination excluding Deaths -----
    data_source_combination_ex_deaths = case_when(
      (stroke_gdppr == 1) & is.na(stroke_hes_apc) & is.na(stroke_ssnap) ~ "GDPPR only",
      is.na(stroke_gdppr) & (stroke_hes_apc == 1) & is.na(stroke_ssnap) ~ "HES-APC only",
      is.na(stroke_gdppr) & is.na(stroke_hes_apc) & (stroke_ssnap == 1) ~ "SSNAP only",
      (stroke_gdppr == 1) & (stroke_hes_apc == 1) & is.na(stroke_ssnap) ~ "GDPPR & HES-APC",
      (stroke_gdppr == 1) & is.na(stroke_hes_apc) & (stroke_ssnap == 1) ~ "GDPPR & SSNAP",
      is.na(stroke_gdppr) & (stroke_hes_apc == 1) & (stroke_ssnap == 1) ~ "HES-APC & SSNAP",
      (stroke_gdppr == 1) & (stroke_hes_apc == 1) & (stroke_ssnap == 1) ~ "GDPPR & HES-APC & SSNAP"
    ) %>% 
      factor() %>% 
      fct_relevel("GDPPR only", "HES-APC only", "SSNAP only", "GDPPR & HES-APC", 
                  "GDPPR & SSNAP", "HES-APC & SSNAP", "GDPPR & HES-APC & SSNAP") %>% 
      ff_label("Data source combination"),
    
    data_source_combination_all = case_when(
      
      (stroke_gdppr == 1) & is.na(stroke_hes_apc) & is.na(stroke_ssnap) & is.na(stroke_deaths) ~ "1: GDPPR only",
      is.na(stroke_gdppr) & (stroke_hes_apc == 1) & is.na(stroke_ssnap) & is.na(stroke_deaths) ~ "1: HES-APC only",
      is.na(stroke_gdppr) & is.na(stroke_hes_apc) & (stroke_ssnap == 1) & is.na(stroke_deaths) ~ "1: SSNAP only",
      is.na(stroke_gdppr) & is.na(stroke_hes_apc) & is.na(stroke_ssnap) & (stroke_deaths == 1) ~ "1: ONS Deaths only",
      
      (stroke_gdppr == 1) & (stroke_hes_apc == 1) & is.na(stroke_ssnap) & is.na(stroke_deaths) ~ "2: GDPPR & HES-APC",
      (stroke_gdppr == 1) & is.na(stroke_hes_apc) & (stroke_ssnap == 1) & is.na(stroke_deaths) ~ "2: GDPPR & SSNAP",
      (stroke_gdppr == 1) & is.na(stroke_hes_apc) & is.na(stroke_ssnap) & (stroke_deaths == 1) ~ "2: GDPPR & ONS Deaths",
      is.na(stroke_gdppr) & (stroke_hes_apc == 1) & (stroke_ssnap == 1) & is.na(stroke_deaths) ~ "2: HES-APC & SSNAP",
      is.na(stroke_gdppr) & (stroke_hes_apc == 1) & is.na(stroke_ssnap) & (stroke_deaths == 1) ~ "2: HES-APC & ONS Deaths",
      is.na(stroke_gdppr) & is.na(stroke_hes_apc) & (stroke_ssnap == 1) & (stroke_deaths == 1) ~ "2: SSNAP & ONS Deaths",
      
      (stroke_gdppr == 1) & (stroke_hes_apc == 1) & (stroke_ssnap == 1) & is.na(stroke_deaths) ~ "3: GDPPR & HES-APC & SSNAP",
      (stroke_gdppr == 1) & (stroke_hes_apc == 1) & is.na(stroke_ssnap) & (stroke_deaths == 1) ~ "3: GDPPR & HES-APC & ONS Deaths",
      (stroke_gdppr == 1) & is.na(stroke_hes_apc) & (stroke_ssnap == 1) & (stroke_deaths == 1) ~ "3: GDPPR & SSNAP & ONS Deaths",
      is.na(stroke_gdppr) & (stroke_hes_apc == 1) & (stroke_ssnap == 1) & (stroke_deaths == 1) ~ "3: HES-APC & SSNAP & ONS Deaths",
      
      (stroke_gdppr == 1) & (stroke_hes_apc == 1) & (stroke_ssnap == 1) & (stroke_deaths == 1) ~ "4: GDPPR & HES-APC & SSNAP & ONS Deaths"
      
    ) %>% 
      factor() %>% 
      fct_relevel(
        "1: GDPPR only","1: HES-APC only", "1: SSNAP only", "1: ONS Deaths only",
        "2: GDPPR & HES-APC", "2: GDPPR & SSNAP", "2: GDPPR & ONS Deaths",
        "2: HES-APC & SSNAP", "2: HES-APC & ONS Deaths", "2: SSNAP & ONS Deaths",
        "3: GDPPR & HES-APC & SSNAP", "3: GDPPR & HES-APC & ONS Deaths",
        "3: GDPPR & SSNAP & ONS Deaths", "3: HES-APC & SSNAP & ONS Deaths",
        "4: GDPPR & HES-APC & SSNAP & ONS Deaths"
        
        
      ) %>% 
      ff_label("Data source combination ")
    
  ) 


# Write cleaned cohort to file
write_rds(cohort, here::here("data", "cohort_cleaned.rds"))
