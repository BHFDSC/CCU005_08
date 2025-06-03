# Databricks notebook source
# MAGIC %run ./project_config

# COMMAND ----------

# MAGIC %run ./D00-parameters

# COMMAND ----------

from hds_functions import load_table, save_table, read_csv_file, map_column_values, first_row
from pyspark.sql import functions as f, DataFrame, Window
from functools import reduce

# COMMAND ----------

primary_care_meds = load_table('primary_care_meds', method = 'primary_care_meds')
cohort = load_table('cohort')

# COMMAND ----------


cohort_primary_care_meds_full = (
    cohort
    .select(
        'row_id', 'person_id', 'index_stroke_date', 'date_of_birth', 'date_of_death', 'discharge_date','stroke_type_harmonised',
        f.to_date(f.lit(study_start_date)).alias('study_start_date'),
        f.to_date(f.lit(study_end_date)).alias('study_end_date')
    )
    .join(
        primary_care_meds
        .select(
            'person_id',
            f.col('processingperioddate').alias('date'),
            f.col('prescribedbnfcode').alias('code'),
            f.col('prescribedbnfname').alias('description')
        )
        .withColumn(
            'medication_group',
            f.when(f.col('code').startswith('01'), f.lit('01: Gastro-Intestinal System'))
            .when(f.col('code').startswith('0201'), f.lit('0201: Positive inotropic drugs'))
            .when(f.col('code').startswith('0202'), f.lit('0202: Diuretics'))
            .when(f.col('code').startswith('0203'), f.lit('0203: Anti-arrhythmic drugs'))
            .when(f.col('code').startswith('0204'), f.lit('0204: Beta-adrenoceptor blocking drugs'))
            .when(f.col('code').startswith('0205'), f.lit('0205: Hypertension and heart failure'))
            .when(f.col('code').startswith('0206'), f.lit('0206: Nitrates, calcium-channel blockers & other antianginal drugs'))
            .when(f.col('code').startswith('0207'), f.lit('0207: Sympathomimetics and other vasoconstrictor drugs'))
            .when(f.col('code').startswith('0208'), f.lit('0208: Anticoagulants and protamine'))
            .when(f.col('code').startswith('0209'), f.lit('0209: Antiplatelet drugs'))
            .when(f.col('code').startswith('0210'), f.lit('0210: Stable angina, acute coronary syndromes and fibrinolysis'))
            .when(f.col('code').startswith('0211'), f.lit('0211: Antifibrinolytic drugs and haemostatics'))
            .when(f.col('code').startswith('0212'), f.lit('0212: Lipid-regulating drugs'))
            .when(f.col('code').startswith('0213'), f.lit('0213: Local sclerosants'))
            .when(f.col('code').startswith('03'), f.lit('03: Respiratory System'))
            .when(f.col('code').startswith('04'), f.lit('04: Central Nervous System'))
            .when(f.col('code').startswith('05'), f.lit('05: Infections'))
            .when(f.col('code').startswith('06'), f.lit('06: Endocrine System'))
            .when(f.col('code').startswith('07'), f.lit('07: Obstetrics, Gynaecology and Urinary-Tract Disorders'))
            .when(f.col('code').startswith('08'), f.lit('08: Malignant Disease and Immunosuppression'))
            .when(f.col('code').startswith('09'), f.lit('09: Nutrition and Blood'))
            .when(f.col('code').startswith('10'), f.lit('10: Musculoskeletal and Joint Diseases'))
            .when(f.col('code').startswith('11'), f.lit('11: Eye'))
            .when(f.col('code').startswith('12'), f.lit('12: Ear, Nose and Oropharynx'))
            .when(f.col('code').startswith('13'), f.lit('13: Skin'))
            .when(f.col('code').startswith('14'), f.lit('14: Immunological Products and Vaccines'))
            .when(f.col('code').startswith('15'), f.lit('15: Anaesthesia'))

            .when(f.col('code').startswith('18'), f.lit('18: Preparations used in Diagnosis'))
            .when(f.col('code').startswith('19'), f.lit('19: Other Drugs and Preparations'))
            .when(f.col('code').startswith('20'), f.lit('20: Dressings'))
            .when(f.col('code').startswith('21'), f.lit('21: Appliances'))
            .when(f.col('code').startswith('22'), f.lit('22: Incontinence Appliances'))
            .when(f.col('code').startswith('23'), f.lit('23: Stoma Appliances'))
            .otherwise('Unknown')
        ),
        on = 'person_id', how = 'inner'
    )
)

save_table(cohort_primary_care_meds_full, table = 'cohort_primary_care_meds_full')

# COMMAND ----------

cohort_primary_care_meds_full = load_table('cohort_primary_care_meds_full')

cohort_primary_care_meds_first_code_ps = (
    cohort_primary_care_meds_full
    .withColumn(
        'min_date',
        f.when(f.col('discharge_date').isNotNull(), f.date_add('discharge_date', 28))
        .otherwise(f.date_add('index_stroke_date', 28))
    )
    .withColumn(
        'max_date',
        f.expr('least(study_end_date, date_of_death, date_add(min_date, 365))')
    )
    .filter("(date >= min_date) AND (date <= max_date)")
    .withColumn(
        'any_antiplatletes',
        f.max(
            f.when(f.col('medication_group').startswith('0209'), f.lit(1))
        ).over(Window.partitionBy('person_id'))
    )
    .transform(
        first_row,
        partition_by = ['row_id', 'person_id', 'code'],
        order_by = ['date']
    )
)

save_table(cohort_primary_care_meds_first_code_ps, table = 'cohort_primary_care_meds_first_code_ps')

# COMMAND ----------

cohort_primary_care_meds_full = load_table('cohort_primary_care_meds_full')

cohort_prior_medications_pmed_long = (
    cohort_primary_care_meds_full
    .select(
        'row_id', 'person_id', 'date', 'code', 'description', 'medication_group',
        f.date_sub('index_stroke_date', 365).alias('min_date'),
        f.col('index_stroke_date').alias('max_date')
    )
    .withColumn(
        'phenotype',
        f.when(f.col('medication_group').startswith('0205'), f.lit('prior_antihypertensives'))
        .when(f.col('medication_group').startswith('0208'), f.lit('prior_anticoagulants'))
        .when(f.col('medication_group').startswith('0209'), f.lit('prior_antiplatelets'))
        .when(f.col('medication_group').startswith('0212'), f.lit('prior_lipid_lowering_drug'))
    )
    .filter("phenotype IS NOT NULL")
    .filter("(date >= min_date) AND (date <= max_date)")
    .withColumn('data_source', f.lit('primary_care_meds'))
    .withColumn('source_priority', f.lit(1))
    .select('row_id', 'person_id', 'phenotype', 'date', 'code', 'description', 'source_priority', 'data_source')
)

cohort_prior_medications_pmed_long = cohort_prior_medications_pmed_long.cache()

cohort_prior_medications_pmed =  (
    cohort_prior_medications_pmed_long
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

save_table(cohort_prior_medications_pmed, table = 'cohort_prior_medications_pmed')


# COMMAND ----------

cohort_primary_care_meds_full = load_table('cohort_primary_care_meds_full')

cohort_ps_medications_pmed_long = (
    cohort_primary_care_meds_full
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
        f.when(f.col('medication_group').startswith('0205'), f.lit('ps_antihypertensives'))
        .when(f.col('medication_group').startswith('0208'), f.lit('ps_anticoagulants'))
        .when(f.col('medication_group').startswith('0209'), f.lit('ps_antiplatelets'))
        .when(f.col('medication_group').startswith('0212'), f.lit('ps_lipid_lowering_drug'))
    )
    .filter("phenotype IS NOT NULL")
    .filter("(date >= min_date) AND (date <= max_date)")
    .withColumn('data_source', f.lit('primary_care_meds'))
    .withColumn('source_priority', f.lit(1))
    .select('row_id', 'person_id', 'phenotype', 'date', 'code', 'description', 'source_priority', 'data_source')
)

cohort_ps_medications_pmed_long = cohort_ps_medications_pmed_long.cache()

cohort_ps_medications_pmed =  (
    cohort_ps_medications_pmed_long
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

save_table(cohort_ps_medications_pmed, table = 'cohort_ps_medications_pmed')


# COMMAND ----------

# Clear cache
spark.catalog.clearCache()

# COMMAND ----------

cohort_prior_medications_pmed = load_table('cohort_prior_medications_pmed')
cohort_ps_medications_pmed = load_table('cohort_ps_medications_pmed')

display(cohort_prior_medications_pmed)
display(cohort_ps_medications_pmed)