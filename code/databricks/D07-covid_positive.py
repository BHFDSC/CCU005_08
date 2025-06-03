# Databricks notebook source
# MAGIC %run ./project_config

# COMMAND ----------

# MAGIC %run ./D00-parameters

# COMMAND ----------

from hds_functions import load_table, save_table, first_row, map_column_values
from pyspark.sql import functions as f

# COMMAND ----------

cohort = load_table('cohort')
display(cohort)

# COMMAND ----------

covid_positive = load_table('covid_positive')
cohort = load_table('cohort')

dict_covid_source_priority = {
    'pillar_2': 1,
    'sgss': 2,
    'hes_apc': 3,
    'gdppr': 4,
    'chess': 5
}

cohort_covid_positive = (
    covid_positive
    .transform(
        map_column_values,
        map_dict = dict_covid_source_priority,
        column = 'data_source',
        new_column = 'source_priority'
    )
    .join(
        cohort
        .select(
            "row_id",
            "person_id",
            f.expr("date_of_birth").alias("min_date"),
            f.expr(f"index_stroke_date").alias("max_date"),
        ),
        on="person_id",
        how="inner",
    )
    .filter("(date >= min_date) AND (date <= max_date)")
    .transform(
        first_row,
        partition_by = ['row_id', 'person_id'],
        order_by = [f.col('date').desc(), f.col('source_priority').asc_nulls_last()]
    )
    .select(
        'row_id',
        'person_id',
        f.lit(1).alias('covid_positive_flag'),
        f.col('date').alias('covid_positive_date'),
        f.col('data_source').alias('covid_positive_source'),
    )
)

save_table(cohort_covid_positive, 'cohort_covid_positive')

# COMMAND ----------
