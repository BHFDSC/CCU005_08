# Databricks notebook source
# MAGIC %md
# MAGIC # D01-stroke_events
# MAGIC
# MAGIC This notebook creates a consolidated table of stroke events from the following sources:
# MAGIC - SSNAP 
# MAGIC - GDDPR (SNOMED codelist)
# MAGIC - HES-APC (primary diagnosis position, ICD-10 codelist)
# MAGIC - ONS Deaths (any position, ICD-10 codelist)
# MAGIC

# COMMAND ----------

# MAGIC %run ./project_config

# COMMAND ----------

from hds_functions import load_table, save_table, read_csv_file, map_column_values
from pyspark.sql import functions as f, DataFrame, Window
from functools import reduce

# COMMAND ----------

# MAGIC %md
# MAGIC # Codelists

# COMMAND ----------


dict_codelist_stroke_icd10 = {
    'ischaemic': './codelists/stroke_ischaemic_icd10.csv',
    'haemorrhagic': './codelists/stroke_haemorrhagic_icd10.csv',
    'nos': './codelists/stroke_nos_icd10.csv',
}

list_codelist_stroke_icd10 = [
    read_csv_file(codelist_path)
    .withColumn('stroke_type', f.lit(stroke_type))
    for stroke_type, codelist_path in dict_codelist_stroke_icd10.items()
]

codelist_stroke_icd10 = reduce(DataFrame.unionByName, list_codelist_stroke_icd10)


# COMMAND ----------

dict_codelist_stroke_snomed = {
    'ischaemic': './codelists/stroke_ischaemic_snomed.csv',
    'haemorrhagic': './codelists/stroke_haemorrhagic_snomed.csv',
    'nos': './codelists/stroke_nos_snomed.csv',
}

list_codelist_stroke_snomed = [
    read_csv_file(codelist_path)
    .withColumn('stroke_type', f.lit(stroke_type))
    for stroke_type, codelist_path in dict_codelist_stroke_snomed.items()
]

codelist_stroke_snomed = reduce(DataFrame.unionByName, list_codelist_stroke_snomed)


# COMMAND ----------

# MAGIC %md
# MAGIC # SSNAP

# COMMAND ----------

ssnap = load_table('ssnap', method = 'ssnap')

ssnap_stroke_events = (
    ssnap
    .select(
        'person_id',
        f.to_date('S1FIRSTARRIVALDATETIME').alias('date'),
        f.col('S2STROKETYPE').alias('code')
    )
    .filter('(person_id IS NOT NULL) AND (date IS NOT NULL)')
    .na.fill({'code': 'missing'})
    .transform(
        map_column_values,
        map_dict = {
            'I': 'Infarction',
            'PIH': 'Primary Intracerebral Haemorrage',
            'missing': 'Missing'
        },
        column = 'code',
        new_column = 'description'
    )
    .transform(
        map_column_values,
        map_dict = {
            'I': 'ischaemic',
            'PIH': 'haemorrhagic',
            'missing': 'missing'
        },
        column = 'code',
        new_column = 'stroke_type'
    )
    .distinct()
    .withColumn('data_source', f.lit('ssnap'))
)

ssnap_stroke_events = ssnap_stroke_events.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC # GDPPR

# COMMAND ----------

gdppr = load_table('gdppr', method = 'gdppr')

gdppr_stroke_events = (
    gdppr
    .select(
        'person_id',
        'date',
        'code'
    )
    .join(
        f.broadcast(codelist_stroke_snomed),
        on = 'code',
        how = 'inner'
    )
)

gdppr_stroke_events = gdppr_stroke_events.cache()

gdppr_stroke_events = (
    gdppr_stroke_events
    .filter('(person_id IS NOT NULL) AND (date IS NOT NULL)')
    .distinct()
    .withColumn('data_source', f.lit('gdppr'))
)

gdppr_stroke_events = gdppr_stroke_events.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC # HES-APC (primary diagnosis)

# COMMAND ----------

hes_apc_diagnosis = load_table('hes_apc_diagnosis')

hes_apc_stroke_events = (
    hes_apc_diagnosis
    .select(
        'person_id',
        f.col('admidate').alias('date'),
        'code',
        'diag_position'
    )
    .filter('diag_position = 1')
    .join(
        f.broadcast(codelist_stroke_icd10),
        on = 'code',
        how = 'inner'
    )
)

hes_apc_stroke_events = hes_apc_stroke_events.cache()

hes_apc_stroke_events = (
    hes_apc_stroke_events
    .filter('(person_id IS NOT NULL) AND (date IS NOT NULL)')
    .drop('diag_position')
    .distinct()
    .withColumn('data_source', f.lit('hes_apc'))
)

hes_apc_stroke_events = hes_apc_stroke_events.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC # ONS Deaths (any position)

# COMMAND ----------

deaths_cause_of_death = load_table('deaths_cause_of_death')

deaths_stroke_events = (
    deaths_cause_of_death
    .select(
        'person_id',
        f.col('date_of_death').alias('date'),
        'cod_position',
        'code'
    )
    .join(
        codelist_stroke_icd10,
        on = 'code', how = 'inner'
    )
    .filter('(person_id IS NOT NULL) AND (date IS NOT NULL)')
    .select('person_id', 'date', 'code', 'description', 'stroke_type')
    .distinct()
    .withColumn('data_source', f.lit('deaths'))
)

deaths_stroke_events = deaths_stroke_events.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC # Combined table

# COMMAND ----------

list_stroke_events = (
    ssnap_stroke_events,
    hes_apc_stroke_events,
    gdppr_stroke_events,
    deaths_stroke_events
)

stroke_events = reduce(DataFrame.unionByName, list_stroke_events)

save_table(df = stroke_events, table = 'stroke_events')

# Clear cache
spark.catalog.clearCache()