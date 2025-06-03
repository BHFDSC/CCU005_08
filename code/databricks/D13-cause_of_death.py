# Databricks notebook source
# MAGIC %run ./project_config

# COMMAND ----------

# MAGIC %run ./D00-parameters

# COMMAND ----------

from hds_functions import load_table, save_table, first_row
from pyspark.sql import functions as f

# COMMAND ----------

cohort = load_table('cohort')
deaths_cause_of_death = load_table('deaths_cause_of_death')

cohort_cause_of_death = (
    cohort
    .select('row_id', 'person_id')
    .join(
        deaths_cause_of_death
        .select('person_id', 'date_of_death', 'cod_position', 'cod_digits', 'code')
        .filter("cod_position = 'underlying'")
        .transform(
            first_row, partition_by = ['person_id'], order_by = ['date_of_death', f.col('cod_digits').desc()]
        ),
        on = 'person_id', how = 'left'
    )
    .select('row_id', 'person_id', f.col('code').alias('cause_of_death_underlying'))
)

save_table(cohort_cause_of_death, 'cohort_cause_of_death')

# Clear cache
spark.catalog.clearCache()