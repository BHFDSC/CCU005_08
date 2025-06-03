# Databricks notebook source
# MAGIC %run ./project_config

# COMMAND ----------

# MAGIC %run ./D00-parameters

# COMMAND ----------

from hds_functions import load_table, save_table, read_csv_file, map_column_values, first_row
from pyspark.sql import functions as f, DataFrame, Window
from functools import reduce

# COMMAND ----------

gdppr = load_table('gdppr', method = 'gdppr')
cohort = load_table('cohort')

# COMMAND ----------

dict_codelist_medication_snomed = {
    'anticoagulants': './codelists/anticoagulants_snomed.csv',
    'antihypertensives': './codelists/antihypertensives_snomed.csv',
    'antiplatelets': './codelists/antiplatelets_snomed.csv',
    'lipid_lowering_drug': './codelists/lipid_lowering_drugs_snomed.csv',
}

list_codelist_medication_snomed = [
    read_csv_file(codelist_path)
    .withColumn('phenotype', f.lit(phenotype))
    for phenotype, codelist_path in dict_codelist_medication_snomed.items()
]

codelist_medication_snomed = reduce(DataFrame.unionByName, list_codelist_medication_snomed)

display(codelist_medication_snomed)


# COMMAND ----------


gdppr_matched = (
    gdppr
    .select('person_id', 'date', 'code')
    .join(
        f.broadcast(codelist_medication_snomed),
        on = 'code', how = 'inner'
    )
)

gdppr_matched = gdppr_matched.cache()

cohort_gdppr_meds_full = (
    gdppr_matched
    .join(
        cohort
        .select(
            'row_id', 'person_id', 'index_stroke_date', 'date_of_birth', 'date_of_death', 'discharge_date','stroke_type_harmonised',
            f.to_date(f.lit(study_start_date)).alias('study_start_date'),
            f.to_date(f.lit(study_end_date)).alias('study_end_date')
        ),
        on = 'person_id', how = 'inner'
    )
)

save_table(cohort_gdppr_meds_full, table = 'cohort_gdppr_meds_full')

# COMMAND ----------

cohort_gdppr_meds_full = load_table('cohort_gdppr_meds_full')

cohort_prior_medications_gdppr_long = (
    cohort_gdppr_meds_full
    .select(
        'row_id', 'person_id', 'date', 'code', 'description', 'phenotype',
        f.date_sub('index_stroke_date', 365).alias('min_date'),
        f.col('index_stroke_date').alias('max_date')
    )
    .withColumn(
        'phenotype',
        f.when(f.col('phenotype') == f.lit('anticoagulants'), f.lit('prior_anticoagulants_gdppr'))
        .when(f.col('phenotype').startswith('antihypertensives'), f.lit('prior_antihypertensives_gdppr'))
        .when(f.col('phenotype').startswith('antiplatelets'), f.lit('prior_antiplatelets_gdppr'))
        .when(f.col('phenotype').startswith('lipid_lowering_drug'), f.lit('prior_lipid_lowering_drug_gdppr'))
    )
    .filter("phenotype IS NOT NULL")
    .filter("(date >= min_date) AND (date <= max_date)")
    .withColumn('data_source', f.lit('gdppr'))
    .withColumn('source_priority', f.lit(1))
    .select('row_id', 'person_id', 'phenotype', 'date', 'code', 'description', 'source_priority', 'data_source')
)

cohort_prior_medications_gdppr_long = cohort_prior_medications_gdppr_long.cache()

cohort_prior_medications_gdppr =  (
    cohort_prior_medications_gdppr_long
    .transform(
        first_row,
        partition_by = ['row_id', 'person_id', 'phenotype'],
        order_by = ['date', 'source_priority', 'code']
    )
    .withColumn('flag', f.lit(1))
    .groupBy('row_id', 'person_id')
    .pivot('phenotype')
    .agg(
        f.first('flag').alias('flag'),
        f.first('date').alias('date'),
        f.first('code').alias('code'),
        f.first('description').alias('description')
    )
)

save_table(cohort_prior_medications_gdppr, table = 'cohort_prior_medications_gdppr')


# COMMAND ----------

cohort_primary_care_meds_full = load_table('cohort_primary_care_meds_full')

cohort_ps_medications_gdppr_long = (
    cohort_gdppr_meds_full
    .withColumn(
        'min_date',
        f.when(f.col('discharge_date').isNotNull(), f.date_add('discharge_date', 28))
        .otherwise(f.date_add('index_stroke_date', 28))
    )
    .withColumn(
        'max_date',
        f.expr('least(study_end_date, date_of_death)')
    )
    .withColumn(
        'phenotype',
        f.when(f.col('phenotype') == f.lit('anticoagulants'), f.lit('ps_anticoagulants_gdppr'))
        .when(f.col('phenotype').startswith('antihypertensives'), f.lit('ps_antihypertensives_gdppr'))
        .when(f.col('phenotype').startswith('antiplatelets'), f.lit('ps_antiplatelets_gdppr'))
        .when(f.col('phenotype').startswith('lipid_lowering_drug'), f.lit('ps_lipid_lowering_drug_gdppr'))
    )
    .filter("phenotype IS NOT NULL")
    .filter("(date >= min_date) AND (date <= max_date)")
    .withColumn('data_source', f.lit('gdppr'))
    .withColumn('source_priority', f.lit(1))
    .select('row_id', 'person_id', 'phenotype', 'date', 'code', 'description', 'source_priority', 'data_source')
)

cohort_ps_medications_gdppr_long = cohort_ps_medications_gdppr_long.cache()

cohort_ps_medications_gdppr =  (
    cohort_ps_medications_gdppr_long
    .transform(
        first_row,
        partition_by = ['row_id', 'person_id', 'phenotype'],
        order_by = ['date', 'source_priority', 'code']
    )
    .withColumn('flag', f.lit(1))
    .groupBy('row_id', 'person_id')
    .pivot('phenotype')
    .agg(
        f.first('flag').alias('flag'),
        f.first('date').alias('date'),
        f.first('code').alias('code'),
        f.first('description').alias('description')
    )
)

save_table(cohort_ps_medications_gdppr, table = 'cohort_ps_medications_gdppr')


# COMMAND ----------

cohort_prior_medications_gdppr = load_table('cohort_prior_medications_gdppr')
cohort_ps_medications_gdppr = load_table('cohort_ps_medications_gdppr')

display(cohort_prior_medications_gdppr)
display(cohort_ps_medications_gdppr)