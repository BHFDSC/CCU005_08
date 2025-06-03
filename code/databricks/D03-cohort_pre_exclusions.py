# Databricks notebook source
# MAGIC %run ./project_config

# COMMAND ----------

# MAGIC %run ./D00-parameters

# COMMAND ----------

from hds_functions import load_table, save_table, first_row, read_csv_file
from pyspark.sql import functions as f, DataFrame
from functools import reduce

# COMMAND ----------

index_stroke = load_table('index_stroke')

index_stroke_selected = (
    index_stroke
    .select(
        [f.monotonically_increasing_id().alias('row_id').cast('string')] + index_stroke.columns
    )
)

index_stroke_selected = index_stroke_selected.cache()


# COMMAND ----------

demographics = load_table('demographics')

demographics_selected = (
    demographics
    .select(
        'person_id', 'date_of_birth', 'sex', 'ethnicity_18_group', 'ethnicity_5_group',
        'death_flag', 'date_of_death', 'in_gdppr', 'gdppr_min_date',
    )
)

demographics_selected = demographics_selected.cache()


# COMMAND ----------

token_pseudo_id_lookup = load_table('token_pseudo_id_lookup')

token_pseudo_id_lookup_selected = (
    token_pseudo_id_lookup
    .select(
        f.col('pseudo_id').alias('person_id'),
        'valid_nhs_number'
    )
)

token_pseudo_id_lookup_selected = token_pseudo_id_lookup_selected.cache()


# COMMAND ----------

prior_stroke = (
    load_table('stroke_events')
    .filter(f"date < '{min_stroke_date}'")
    .groupBy('person_id')
    .agg(f.lit(1).alias('prior_stroke'))
)

prior_stroke = prior_stroke.cache()

# COMMAND ----------

lsoa_closest_to_stroke_date = (
    load_table('lsoa_multisource')
    .filter(f.col('data_source') != f.lit('ssnap'))
    .join(
        index_stroke_selected
        .select('person_id', 'index_stroke_date'),
        on = 'person_id',
        how = 'inner',
    )
    .withColumn('date_diff', f.datediff('index_stroke_date', 'record_date'))
    .withColumn('abs_date_diff', f.abs('date_diff'))
    .filter("date_diff >= -180")
    .transform(
        first_row, n = 1,
        partition_by = ['person_id'],
        order_by = ['abs_date_diff']
    )
    .select('person_id', 'lsoa', f.col('record_date').alias('lsoa_date'))
)

lsoa_closest_to_stroke_date = lsoa_closest_to_stroke_date.cache()


# COMMAND ----------


dict_codelist_sex_specific_markers_snomed = {
    'pregnancy_or_childbirth': './codelists/pregnancy_or_childbirth_snomed.csv',
    'prostate_cancer': './codelists/prostate_cancer_snomed.csv',
}

list_codelist_sex_specific_markers_snomed = [
    read_csv_file(codelist_path)
    .withColumn('phenotype', f.lit(phenotype))
    for phenotype, codelist_path in dict_codelist_sex_specific_markers_snomed.items()
]

codelist_sex_specific_markers_snomed = reduce(DataFrame.unionByName, list_codelist_sex_specific_markers_snomed)

gdppr = load_table('gdppr', method = 'gdppr')

gdppr_sex_specific_markers = (
    gdppr
    .select('person_id', 'code', 'date', 'record_date')
    .withColumn(
        'date',
        f.when(f.col('date').isNull(), f.col('record_date'))
        .otherwise(f.col('date'))
    )
    .join(
        f.broadcast(
            codelist_sex_specific_markers_snomed
            .select('code', 'phenotype')
        ),
        on = 'code', how = 'inner'
    )
)

gdppr_sex_specific_markers = gdppr_sex_specific_markers.cache()

gdppr_sex_specific_markers = (
    gdppr_sex_specific_markers
    .filter("date IS NOT NULL")
    .join(
        f.broadcast(
            index_stroke_selected
            .select('person_id')
        ),
        on = 'person_id', how = 'inner'
    )
    .groupBy('person_id')
    .pivot('phenotype')
    .agg(
        f.lit(1).alias('flag')
    )
)

gdppr_sex_specific_markers = gdppr_sex_specific_markers.cache()


# COMMAND ----------


dict_codelist_sex_specific_markers_bnf = {
    'cocp': './codelists/combined_oral_contraceptive_pill_bnf.csv',
    'hrt': './codelists/hormone_replacement_therapy_bnf.csv',
}

list_codelist_sex_specific_markers_bnf = [
    read_csv_file(codelist_path)
    .withColumn('phenotype', f.lit(phenotype))
    for phenotype, codelist_path in dict_codelist_sex_specific_markers_bnf.items()
]

codelist_sex_specific_markers_bnf = reduce(DataFrame.unionByName, list_codelist_sex_specific_markers_bnf)

primary_care_meds = load_table('primary_care_meds', method = 'primary_care_meds')

primary_care_meds_sex_specific_markers = (
    primary_care_meds
    .select(
        'person_id',
        f.col('processingperioddate').alias('date'),
        f.col('prescribedbnfcode').alias('code')
    )
    .join(
        f.broadcast(
            codelist_sex_specific_markers_bnf
            .select('code', 'phenotype')
        ),
        on = 'code', how = 'inner'
    )
)

primary_care_meds_sex_specific_markers = primary_care_meds_sex_specific_markers.cache()

primary_care_meds_sex_specific_markers = (
    primary_care_meds_sex_specific_markers
    .filter("date IS NOT NULL")
    .join(
        f.broadcast(
            index_stroke_selected
            .select('person_id')
        ),
        on = 'person_id', how = 'inner'
    )
    .groupBy('person_id')
    .pivot('phenotype')
    .agg(
        f.lit(1).alias('flag')
    )
)

primary_care_meds_sex_specific_markers = primary_care_meds_sex_specific_markers.cache()


# COMMAND ----------

gdppr = load_table('gdppr', method = 'gdppr')

gdppr_ids_with_empty_dates = (
    gdppr
    .select('person_id', 'date', 'record_date')
    .filter("(person_id IS NOT NULL) AND (date IS NULL) AND (record_date IS NULL)")
    .distinct()
)

gdppr_ids_with_empty_dates = gdppr_ids_with_empty_dates.cache()

gdppr_records_of_ids_with_empty_dates = (
    gdppr
    .select('person_id', 'date', 'record_date')
    .join(
        gdppr_ids_with_empty_dates
        .select('person_id'),
        on = 'person_id', how = 'inner'
    )
)

gdppr_records_of_ids_with_empty_dates = gdppr_records_of_ids_with_empty_dates.cache()

gdppr_ids_with_only_empty_dates = (
    gdppr_records_of_ids_with_empty_dates
    .withColumn(
        'flag_both_dates_null',
        f.when(
            f.col('date').isNull() & f.col('record_date').isNull(),
            f.lit(True)
        )
        .otherwise(f.lit(False))
    )
    .groupBy('person_id')
    .agg(
        f.when(
            f.min('flag_both_dates_null') == f.lit(True), f.lit(True)
        )
        .otherwise(f.lit(None)).alias('id_with_only_empty_dates')
    )
)

gdppr_ids_with_only_empty_dates = gdppr_ids_with_only_empty_dates.cache()

# COMMAND ----------

hes_apc_cips_episodes = load_table('hes_apc_cips_episodes')

hes_apc_length_of_stay = (
    hes_apc_cips_episodes
    .select('person_id', 'admidate', 'disdate', 'cips_disdate')
    .withColumn(
        'disdate',
        f.when(
            (f.col('disdate') != f.lit('1801-01-01'))
            & (f.col('disdate') != f.lit('1801-01-01')),
            f.col('disdate')
        )
    )
    .join(
        index_stroke_selected
        .select('person_id', 'index_stroke_date'),
        on = 'person_id', how = 'inner'
    )
    .filter(
        (f.col('admidate') >= f.col('index_stroke_date'))
        & (f.col('admidate') <= f.date_add('index_stroke_date', 30))
    )
    .transform(
        first_row,
        partition_by = ['person_id'],
        order_by = [f.col('admidate').asc_nulls_last(), f.col('disdate').asc_nulls_last()]
    )
    .withColumn(
        'length_of_stay_hes_apc',
        f.when(
            f.col('admidate') <= f.col('disdate'),
            f.datediff(f.col('disdate'), f.col('admidate'))
        )
    )
    .select(
        'person_id',
        f.col('admidate').alias('admission_date_hes_apc'),
        f.col('disdate').alias('discharge_date_hes_apc'),
        'length_of_stay_hes_apc'
    )
)

hes_apc_length_of_stay = hes_apc_length_of_stay.cache()



# COMMAND ----------

ssnap = load_table('ssnap', method = 'ssnap')

ssnap_length_of_stay = (
    ssnap
    .select(
        'person_id', 
        f.to_date('s1firstarrivaldatetime').alias('admission_date_ssnap'), 
        f.to_date('s7hospitaldischargedatetime').alias('discharge_date_ssnap')
    )
    .join(
        index_stroke_selected
        .select('person_id', 'index_stroke_date'),
        on = 'person_id', how = 'inner'
    )
    .filter(
        (f.col('admission_date_ssnap') >= f.col('index_stroke_date'))
        & (f.col('admission_date_ssnap') <= f.date_add('index_stroke_date', 30))
    )
    .transform(
        first_row,
        partition_by = ['person_id'],
        order_by = [f.col('admission_date_ssnap').asc_nulls_last(), f.col('discharge_date_ssnap').asc_nulls_last()]
    )
    .withColumn(
        'length_of_stay_ssnap',
        f.when(
            f.col('admission_date_ssnap') <= f.col('discharge_date_ssnap'),
            f.datediff(f.col('discharge_date_ssnap'), f.col('admission_date_ssnap'))
        )
    )
    .select(
        'person_id', 'admission_date_ssnap', 'discharge_date_ssnap', 'length_of_stay_ssnap'
    )
)

ssnap_length_of_stay = ssnap_length_of_stay.cache()


# COMMAND ----------


list_dataframes = [
    index_stroke_selected,
    demographics_selected,
    token_pseudo_id_lookup_selected,
    prior_stroke,
    lsoa_closest_to_stroke_date,
    gdppr_sex_specific_markers,
    primary_care_meds_sex_specific_markers,
    gdppr_ids_with_only_empty_dates,
    hes_apc_length_of_stay,
    ssnap_length_of_stay
]

cohort_pre_exclusions = reduce(lambda df1, df2: df1.join(df2, on='person_id', how='left'), list_dataframes)

cohort_pre_exclusions = cohort_pre_exclusions.cache()

cohort_pre_exclusions = (
    cohort_pre_exclusions
    .withColumn(
        'age_on_stroke_date',
        f.round((f.datediff(f.col('index_stroke_date'), f.col('date_of_birth')) / 365.25), 2)
    )
    .withColumn(
        'discharge_date',
        f.when(f.col('discharge_date_hes_apc').isNotNull(), f.col('discharge_date_hes_apc'))
        .when(f.col('discharge_date_ssnap').isNotNull(), f.col('discharge_date_ssnap'))
    )
    .withColumn(
        'length_of_stay',
        f.when(f.col('length_of_stay_hes_apc').isNotNull(), f.col('length_of_stay_hes_apc'))
        .when(f.col('length_of_stay_ssnap').isNotNull(), f.col('length_of_stay_ssnap'))
    )
)

save_table(df = cohort_pre_exclusions, table = 'cohort_pre_exclusions')


# COMMAND ----------

# Clear cache
spark.catalog.clearCache()

# COMMAND ----------

cohort_pre_exclusions = load_table('cohort_pre_exclusions')
display(cohort_pre_exclusions)