# Databricks notebook source
# MAGIC %run ./project_config

# COMMAND ----------

# MAGIC %run ./D00-parameters

# COMMAND ----------

from hds_functions import load_table, save_table, read_csv_file, map_column_values, first_row
from pyspark.sql import functions as f, DataFrame
from functools import reduce

# COMMAND ----------

dict_codelist_measures_snomed = {
    'body_mass_index': './codelists/body_mass_index_snomed.csv',
    'systolic_blood_pressure': './codelists/systolic_blood_pressure_snomed.csv',
    'egfr': './codelists/egfr_snomed.csv',
    'glycated_haemoglobin': './codelists/glycated_haemoglobin_snomed.csv',
    'total_cholesterol': './codelists/total_cholesterol_snomed.csv',
    'hdl_cholesterol': './codelists/hdl_cholesterol_snomed.csv'
}

list_codelist_measures_snomed = [
    read_csv_file(codelist_path)
    .withColumn('phenotype', f.lit(phenotype))
    for phenotype, codelist_path in dict_codelist_measures_snomed.items()
]

codelist_measures_snomed = reduce(DataFrame.unionByName, list_codelist_measures_snomed)


# COMMAND ----------

min_value_dict = {
    "body_mass_index": 5,
    "systolic_blood_pressure": 30,
    "egfr": 0,
    "glycated_haemoglobin": 3,
    "total_cholesterol": 0.5,
    "hdl_cholesterol": 0.1
}

max_value_dict = {
    "body_mass_index": 100,
    "systolic_blood_pressure": 300,
    "egfr": 200,
    "glycated_haemoglobin": 250,
    "total_cholesterol": 30,
    "hdl_cholesterol": 10
}

codelist_measures_snomed = (
    codelist_measures_snomed
    .transform(
        map_column_values,
        map_dict = min_value_dict, column = 'phenotype', new_column = 'min_value'
    )
    .transform(
        map_column_values,
        map_dict = max_value_dict, column = 'phenotype', new_column = 'max_value'
    )
)

codelist_measures_snomed.cache()

display(codelist_measures_snomed)

# COMMAND ----------

gdppr = load_table('gdppr', method = 'gdppr')
cohort = load_table('cohort')

gdppr_matched = (
    gdppr
    .select('person_id', 'date', 'code', f.col('value1_condition').alias('value'))
    .join(
        f.broadcast(codelist_measures_snomed),
        on = 'code', how = 'inner'
    )
    .filter(
        ((f.col('value') >= f.col('min_value')) | f.col('min_value').isNull())
        & ((f.col('value') <= f.col('max_value')) | f.col('max_value').isNull())
        & (f.col('value').isNotNull())
    )
)

gdppr_matched.cache()

cohort_measures = (
    gdppr_matched
    .join(
        cohort
        .select(
            'row_id', 'person_id',
            f.expr('date_add(index_stroke_date, -2*365)').alias('min_date'),
            f.expr('index_stroke_date').alias('max_date')
        ),
        on = 'person_id', how = 'inner'
    )
    .filter("(date >= min_date) AND (date <= max_date)")
    .transform(
        first_row,
        partition_by = ['row_id', 'person_id', 'phenotype'],
        order_by = [f.col('date').desc(), 'code']
    )
    .withColumn('flag', f.lit(1))
    .withColumn('source', f.lit('gdppr'))
    .groupBy('row_id', 'person_id')
    .pivot('phenotype')
    .agg(
        f.first('date').alias('date'),
        f.first('value').alias('value')
    )
)

save_table(df = cohort_measures, table = 'cohort_measures')

# Clear cache
spark.catalog.clearCache()


# COMMAND ----------

cohort_measures = load_table('cohort_measures')
display(cohort_measures)