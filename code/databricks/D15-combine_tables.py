# Databricks notebook source
# MAGIC %run ./project_config

# COMMAND ----------

# MAGIC %run ./D00-parameters

# COMMAND ----------

from hds_functions import load_table, save_table, first_row, read_csv_file
from pyspark.sql import functions as f, DataFrame
from functools import reduce

# COMMAND ----------

cohort = load_table('cohort')
cohort_region_imd = load_table('cohort_region_imd')
cohort_home_time = load_table('cohort_home_time') 
cohort_covid_positive  = load_table('cohort_covid_positive') 
cohort_measures = load_table('cohort_measures') 
cohort_comorbidities = load_table('cohort_comorbidities') 
cohort_prior_medications_pmed = load_table('cohort_prior_medications_pmed') 
cohort_ps_medications_pmed = load_table('cohort_ps_medications_pmed') 
cohort_prior_medications_gdppr = load_table('cohort_prior_medications_gdppr') 
cohort_ps_medications_gdppr = load_table('cohort_ps_medications_gdppr') 
cohort_ssnap_variables = load_table('cohort_ssnap_variables') 
cohort_cause_of_death = load_table('cohort_cause_of_death')
cohort_smoking = load_table('cohort_smoking')

list_cohort_tables = [
    cohort,
    cohort_region_imd,
    cohort_home_time,
    cohort_covid_positive,
    cohort_measures,
    cohort_comorbidities,
    cohort_prior_medications_pmed,
    cohort_ps_medications_pmed,
    cohort_prior_medications_gdppr,
    cohort_ps_medications_gdppr,
    cohort_ssnap_variables,
    cohort_cause_of_death,
    cohort_smoking
]

cohort_final = reduce(lambda df1, df2: df1.join(df2, on = ['row_id', 'person_id'], how='left'), list_cohort_tables)

save_table(cohort_final, 'cohort_final')

# Clear cache
spark.catalog.clearCache()

# COMMAND ----------

cohort_final = load_table('cohort_final')
display(cohort_final)
display(
    cohort_final
    .agg(
        f.count('*'),
        f.countDistinct('row_id'),
        f.countDistinct('person_id')
    )
)