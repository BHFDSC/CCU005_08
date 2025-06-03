# Databricks notebook source
# MAGIC %md
# MAGIC # D04-cohort_filtered
# MAGIC
# MAGIC This notebook filters the cohort table based on the specified inclusion criteria

# COMMAND ----------

# MAGIC %run ./project_config

# COMMAND ----------

# MAGIC %run ./D00-parameters

# COMMAND ----------

from hds_functions import load_table, save_table, apply_inclusion_criteria
from pyspark.sql import functions as f

# COMMAND ----------

inclusion_criteria = {
    "alive_on_stroke_date": "(date_of_death IS NULL) OR (date_of_death >= index_stroke_date)",
    "with_valid_nhs_number": "valid_nhs_number",
    "record_in_gdppr": "in_gdppr = 1",
    "with_no_prior_stroke": "prior_stroke IS NULL",
    "valid_date_of_birth": "(date_of_birth > '1793-01-01') AND (date_of_birth < current_date())",
    "valid_age_on_stroke_date": "(age_on_stroke_date >= 18) AND (age_on_stroke_date <= 120)",
    "valid_date_of_death": """
        (death_flag IS NULL)
        OR (
            (death_flag = 1) AND (date_of_death >= '1900-01-01') AND (date_of_death < current_date())
            AND (date_of_death >= date_of_birth)
        )
    """,
    "valid_lsoa_in_england": "lsoa LIKE 'E01%'",
    "gdppr_id_with_at_least_one_valid_date": "id_with_only_empty_dates IS NULL",
    "known_sex_male_or_female": "(sex = 'M') OR (sex = 'F')",
    "no_conflicts_with_sex_specific_markers": """
        ((sex = 'F') AND (prostate_cancer IS NULL))
        OR ((sex = 'M') AND (pregnancy_or_childbirth IS NULL) AND (cocp IS NULL) AND (hrt IS NULL))
    """
}

# COMMAND ----------

cohort_pre_exclusions = load_table('cohort_pre_exclusions')

cohort = apply_inclusion_criteria(
    cohort_pre_exclusions,
    inclusion_criteria,
    flowchart_table = 'cohort_inclusion_flowchart'
)

save_table(cohort, 'cohort')

# COMMAND ----------

cohort_inclusion_flowchart = load_table('cohort_inclusion_flowchart')
display(cohort_inclusion_flowchart)