# Databricks notebook source
# MAGIC %run ./project_config

# COMMAND ----------

# MAGIC %run ./D00-parameters

# COMMAND ----------

from hds_functions import load_table, save_table, read_csv_file, map_column_values, first_row
from pyspark.sql import functions as f, DataFrame, Window
from functools import reduce

# COMMAND ----------

ssnap = load_table('ssnap', method = 'ssnap')
cohort = load_table('cohort')

cohort_ssnap_variables = (
    cohort
    .select('row_id', 'person_id', 'index_stroke_date')
    .join(
        ssnap
        .select(
            'person_id',
            f.to_date('s1firstarrivaldatetime').alias('date'),
            f.col('s2nihssarrival').alias('nihss_arrival'),
            f.col('s2rankinbeforestroke').alias('rankin_before_stroke'),
            f.col('s7rankindischarge').alias('rankin_discharge')
        ),
        on = 'person_id', how = 'inner'
    )
    .filter("(date >= index_stroke_date) AND (date <= date_add(index_stroke_date, 30))")
    .transform(
        first_row,
        partition_by = ['row_id', 'person_id'],
        order_by = ['date'],
    )
    .select('row_id', 'person_id', 'nihss_arrival', 'rankin_before_stroke', 'rankin_discharge')
)

save_table(cohort_ssnap_variables, 'cohort_ssnap_variables')

# COMMAND ----------

cohort_ssnap_variables = load_table('cohort_ssnap_variables')
display(cohort_ssnap_variables)