# Databricks notebook source
# MAGIC %run ./project_config

# COMMAND ----------

# MAGIC %run ./D00-parameters

# COMMAND ----------

from hds_functions import load_table, save_table, read_csv_file, map_column_values, first_row
from pyspark.sql import functions as f, DataFrame
from functools import reduce

# COMMAND ----------

dict_codelist_comorbidities_snomed = {
    'angina': './codelists/angina_snomed.csv',
    'atrial_fibrillation': './codelists/atrial_fibrillation_snomed.csv',
    'cancer': './codelists/cancer_snomed.csv',
    'chronic_kidney_disease': './codelists/chronic_kidney_disease_snomed.csv',
    'copd': './codelists/copd_snomed.csv',
    'deep_vein_thrombosis': './codelists/deep_vein_thrombosis_snomed.csv',
    'dementia': './codelists/dementia_snomed.csv',
    'depression': './codelists/depression_snomed.csv',
    'diabetes': './codelists/diabetes_snomed.csv',
    'hypercholesterolaemia': './codelists/hypercholesterolaemia_snomed.csv',
    'hypertension': './codelists/hypertension_snomed.csv',
    'liver_disease': './codelists/liver_disease_snomed.csv',
    'myocardial_infarction': './codelists/myocardial_infarction_snomed.csv',
    'obesity': './codelists/obesity_snomed.csv'
}

list_codelist_comorbidities_snomed = [
    read_csv_file(codelist_path)
    .withColumn('phenotype', f.lit(phenotype))
    for phenotype, codelist_path in dict_codelist_comorbidities_snomed.items()
]

codelist_comorbidities_snomed = reduce(DataFrame.unionByName, list_codelist_comorbidities_snomed)

display(codelist_comorbidities_snomed)


# COMMAND ----------

dict_codelist_comorbidities_icd10 = {
    'angina': './codelists/angina_icd10.csv',
    'arrhythmia': './codelists/arrhythmia_icd10.csv',
    'atrial_fibrillation': './codelists/atrial_fibrillation_icd10.csv',
    'cancer': './codelists/cancer_icd10.csv',
    'chronic_kidney_disease': './codelists/chronic_kidney_disease_icd10.csv',
    'copd': './codelists/copd_icd10.csv',
    'deep_vein_thrombosis': './codelists/deep_vein_thrombosis_icd10.csv',
    'dementia': './codelists/dementia_icd10.csv',
    'depression': './codelists/depression_icd10.csv',
    'diabetes': './codelists/diabetes_icd10.csv',
    'hypertension': './codelists/hypertension_icd10.csv',
    'liver_disease': './codelists/liver_disease_icd10.csv',
    'myocardial_infarction': './codelists/myocardial_infarction_icd10.csv',
    'obesity': './codelists/obesity_icd10.csv',

    # Charlson comorbidity index
    'cci_aids': './codelists/cci_aids_icd10.csv',
    'cci_ami': './codelists/cci_ami_icd10.csv',
    'cci_canc': './codelists/cci_canc_icd10.csv',
    'cci_cevd': './codelists/cci_cevd_icd10.csv',
    'cci_chf': './codelists/cci_chf_icd10.csv',
    'cci_cpd': './codelists/cci_cpd_icd10.csv',
    'cci_dementia': './codelists/cci_dementia_icd10.csv',
    'cci_diab': './codelists/cci_diab_icd10.csv',
    'cci_diabwc': './codelists/cci_diabwc_icd10.csv',
    'cci_hp': './codelists/cci_hp_icd10.csv',
    'cci_metacanc': './codelists/cci_metacanc_icd10.csv',
    'cci_mld': './codelists/cci_mld_icd10.csv',
    'cci_msld': './codelists/cci_msld_icd10.csv',
    'cci_pud': './codelists/cci_pud_icd10.csv',
    'cci_pvd': './codelists/cci_pvd_icd10.csv',
    'cci_rend': './codelists/cci_rend_icd10.csv',
    'cci_rheumd': './codelists/cci_rheumd_icd10.csv'

}

list_codelist_comorbidities_icd10 = [
    read_csv_file(codelist_path)
    .withColumn('phenotype', f.lit(phenotype))
    for phenotype, codelist_path in dict_codelist_comorbidities_icd10.items()
]

codelist_comorbidities_icd10 = reduce(DataFrame.unionByName, list_codelist_comorbidities_icd10)

display(codelist_comorbidities_icd10)


# COMMAND ----------

hes_apc_diagnosis = load_table("hes_apc_diagnosis")
cohort = load_table("cohort")

hes_apc_matched = (
    hes_apc_diagnosis
    .select("person_id", f.col("epistart").alias("date"), "code")
    .join(
        f.broadcast(
          codelist_comorbidities_icd10
          .select("code", "phenotype")
        ),
        on = "code", how = "inner",
    )
)

hes_apc_matched = hes_apc_matched.cache()

cohort_comorbidities_hes_apc = (
    hes_apc_matched
    .join(
        cohort
        .select(
            "row_id",
            "person_id",
            f.expr("date_of_birth").alias("min_date"),
            f.expr("index_stroke_date").alias("max_date"),
        ),
        on="person_id",
        how="inner",
    )
    .filter("(date >= min_date) AND (date <= max_date)")
    .withColumn("data_source", f.lit("hes_apc"))
    .withColumn("source_priority", f.lit(1))
    .select('row_id', 'person_id', 'phenotype', 'date', 'code', 'data_source', 'source_priority')
)

cohort_comorbidities_hes_apc = cohort_comorbidities_hes_apc.cache()


# COMMAND ----------

gdppr = load_table("gdppr", method = 'gdppr')
cohort = load_table("cohort")

gdppr_matched = (
    gdppr
    .select("person_id", "date", "code")
    .join(
        f.broadcast(
          codelist_comorbidities_snomed
          .select("code", "phenotype")
        ),
        on = "code", how = "inner",
    )
)

gdppr_matched = gdppr_matched.cache()

cohort_comorbidities_gdppr = (
    gdppr_matched
    .join(
        cohort
        .select(
            "row_id",
            "person_id",
            f.expr("date_of_birth").alias("min_date"),
            f.expr("index_stroke_date").alias("max_date"),
        ),
        on="person_id",
        how="inner",
    )
    .filter("(date >= min_date) AND (date <= max_date)")
    .withColumn("data_source", f.lit("gdppr"))
    .withColumn("source_priority", f.lit(2))
    .select('row_id', 'person_id', 'phenotype', 'date', 'code', 'data_source', 'source_priority')
)

cohort_comorbidities_gdppr = cohort_comorbidities_gdppr.cache()

# COMMAND ----------


list_cohort_comorbidities = [
    cohort_comorbidities_hes_apc,
    cohort_comorbidities_gdppr
]

cohort_comorbidities_combined = reduce(DataFrame.unionByName, list_cohort_comorbidities)

cohort_comorbidities_combined = cohort_comorbidities_combined.cache()

# COMMAND ----------


cohort_comorbidities = (
    cohort_comorbidities_combined
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
    )
)

save_table(cohort_comorbidities, table = 'cohort_comorbidities')

# COMMAND ----------

cohort_comorbidities = load_table('cohort_comorbidities')

display(cohort_comorbidities)