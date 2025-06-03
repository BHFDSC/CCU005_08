# Databricks notebook source
# MAGIC %run ./project_config

# COMMAND ----------

# MAGIC %run ./D00-parameters

# COMMAND ----------

from hds_functions import load_table, save_table, read_csv_file, clean_column_names
from pyspark.sql import functions as f

# COMMAND ----------

cohort_lsoa = (
    load_table('cohort')
    .select('row_id', 'person_id', 'lsoa')
)

# COMMAND ----------

lsoa_region_mapping = (
    read_csv_file(
        path = 'lsoa_mapping/lsoa11_buasd11_bua11_rgn11_best_fit_lookup_england_wales.csv',
        repo = 'hds_reference_data'
    )
    .transform(clean_column_names)
    .select(f.col('lsoa11cd').alias('lsoa'), f.col('rgn11nm').alias('region'))
)


# COMMAND ----------

lsoa_imd19_mapping = (
    read_csv_file(
        path = 'lsoa_mapping/lsoa11_imd19_OSGB1936.csv',
        repo = 'hds_reference_data'
    )
    .select(
        f.col('lsoa11cd').alias('lsoa'),
        f.col('IMDDecil').alias('imd_19_decil'),
    )
    .withColumn(
        'imd_19_decile',
        f.when(f.col('imd_19_decil') == 1, '1 (most deprived)')
        .when(f.col('imd_19_decil') == 10, '10 (least deprived)')
        .otherwise(f.col('imd_19_decil').cast('string'))
    )
    .withColumn(
        'imd_19_quintile',
        f.when((f.col('imd_19_decil') == 1) | (f.col('imd_19_decil') == 2), '1 (most deprived)')
        .when((f.col('imd_19_decil') == 3) | (f.col('imd_19_decil') == 4), '2')
        .when((f.col('imd_19_decil') == 5) | (f.col('imd_19_decil') == 6), '3')
        .when((f.col('imd_19_decil') == 7) | (f.col('imd_19_decil') == 8), '4')
        .when((f.col('imd_19_decil') == 9) | (f.col('imd_19_decil') == 10), '5 (least deprived)')
    )
    .select('lsoa', 'imd_19_decile', 'imd_19_quintile')
)


# COMMAND ----------

cohort_region_imd = (
    cohort_lsoa
    .join(
        lsoa_region_mapping,
        on = 'lsoa', how = 'left'
    )
    .join(
        lsoa_imd19_mapping,
        on = 'lsoa', how = 'left'
    )
    .select('row_id', 'person_id', 'region', 'imd_19_decile', 'imd_19_quintile')
)

save_table(cohort_region_imd, 'cohort_region_imd')