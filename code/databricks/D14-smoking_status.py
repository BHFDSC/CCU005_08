# Databricks notebook source
# MAGIC %run ./project_config

# COMMAND ----------

# MAGIC %run ./D00-parameters

# COMMAND ----------

from hds_functions import load_table, save_table, read_csv_file, map_column_values, first_row
from pyspark.sql import functions as f, DataFrame
from functools import reduce

# COMMAND ----------

dict_codelist_smoking_snomed = {
    'smoking_status_current': './codelists/smoking_status_current_snomed.csv',
    'smoking_status_ex': './codelists/smoking_status_ex_snomed.csv',
    'smoking_status_never': './codelists/smoking_status_never_snomed.csv'
}

list_codelist_smoking_snomed = [
    read_csv_file(codelist_path)
    .withColumn('phenotype', f.lit(phenotype))
    for phenotype, codelist_path in dict_codelist_smoking_snomed.items()
]

codelist_smoking_snomed = reduce(DataFrame.unionByName, list_codelist_smoking_snomed)

codelist_smoking_snomed.cache()

display(codelist_smoking_snomed)


# COMMAND ----------

gdppr = load_table('gdppr', method = 'gdppr')
cohort = load_table('cohort')

gdppr_matched = (
    gdppr
    .select('person_id', 'date', 'code')
    .join(
        f.broadcast(codelist_smoking_snomed),
        on = 'code', how = 'inner'
    )
)

gdppr_matched.cache()

cohort_smoking = (
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
        f.first('flag').alias('flag'),
        f.first('date').alias('date')
    )
)

save_table(df = cohort_smoking, table = 'cohort_smoking')

# Clear cache
spark.catalog.clearCache()