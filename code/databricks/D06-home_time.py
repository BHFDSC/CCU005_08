# Databricks notebook source
# MAGIC %run ./project_config

# COMMAND ----------

# MAGIC %run ./D00-parameters

# COMMAND ----------

from hds_functions import load_table, save_table
from pyspark.sql import functions as f, Window

# COMMAND ----------

cohort = load_table('cohort')

follow_up_duration = 180

cohort_selected = (
    cohort
    .select('row_id', 'person_id', 'index_stroke_date', 'date_of_death')
    .withColumn('follow_up_duration', f.lit(follow_up_duration))
    .withColumn('follow_up_end_date', f.date_add('index_stroke_date', follow_up_duration))
)

display(cohort_selected)

cohort_selected = cohort_selected.cache()

# COMMAND ----------

hes_apc = load_table('hes_apc', method = 'hes_apc')

hes_apc_agg = (
    cohort_selected
    .join(
        hes_apc
        .select('person_id', 'admidate', 'epiend', 'disdate')
        .withColumn(
            'admidate',
            f.when(
                (f.col('admidate') != f.to_date(f.lit('1800-01-01')))
                & (f.col('admidate') != f.to_date(f.lit('1801-01-01'))),
                f.col('admidate')
            )
        )
        .withColumn(
            'disdate',
            f.when(
                (f.col('disdate') != f.to_date(f.lit('1800-01-01')))
                & (f.col('disdate') != f.to_date(f.lit('1801-01-01'))),
                f.col('disdate')
            )
        )
        .withColumn(
            'epiend',
            f.when(
                (f.col('epiend') != f.to_date(f.lit('1800-01-01')))
                & (f.col('epiend') != f.to_date(f.lit('1801-01-01'))),
                f.col('epiend')
            )
        )
        .filter(f.col('admidate').isNotNull())
        .groupBy('person_id', 'admidate')
        .agg(
            f.max('disdate').alias('disdate'),
            f.max('epiend').alias('max_epiend')
        )
        .withColumn(
            'disdate',
            f.when(f.col('disdate').isNull(), f.col('max_epiend'))
            .otherwise(f.col('disdate'))
        ),
        on = 'person_id', how = 'inner'
    )
)

hes_apc_filtered = (
    hes_apc_agg
    .filter(
        (f.col('disdate') >= f.col('index_stroke_date'))
        & (f.col('admidate') <= f.col('disdate'))
        & (f.col('admidate') <= f.col('follow_up_end_date'))
        & (f.col('admidate') <= f.col('date_of_death'))
    )
)

hes_apc_filtered = hes_apc_filtered.cache()

display(hes_apc_filtered)


# COMMAND ----------

window_rolling_max = (
    Window.partitionBy('row_id')
    .orderBy('admidate', 'disdate')
    .rowsBetween(Window.unboundedPreceding, Window.currentRow - 1)
)

window_spell_marker = (
    Window.partitionBy('row_id')
    .orderBy('admidate', 'disdate')
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

window_spell = Window.partitionBy('row_id', 'spell_marker')


hes_apc_spells_marked = (
    hes_apc_filtered
    .withColumn('rolling_max_disdate', f.max('disdate').over(window_rolling_max))
    .withColumn(
        'new_spell_flag',
        f.when(
            (f.col('admidate') <= f.col('rolling_max_disdate')),
            f.lit(0)
        )
        .otherwise(1),
    )
    .withColumn('spell_marker', f.sum('new_spell_flag').over(window_spell_marker))
    .withColumn('spell_start_date', f.min('admidate').over(window_spell))
    .withColumn('spell_end_date', f.max('disdate').over(window_spell))
)

hes_apc_spells_marked = hes_apc_spells_marked.cache()

display(hes_apc_spells_marked)

# COMMAND ----------

hes_apc_spells_consolidated = (
    hes_apc_spells_marked
    .select(
        'row_id', 'person_id', 'index_stroke_date', 'date_of_death', 'follow_up_end_date',
        'spell_start_date', 'spell_end_date'
    )
    .withColumn('spell_end_date_truncated', f.least('spell_end_date', 'date_of_death', 'follow_up_end_date'))
    .withColumn('spell_start_date_truncated', f.greatest('spell_start_date', 'index_stroke_date'))
    .select(
        'row_id', 'person_id', 'index_stroke_date', 'date_of_death', 'follow_up_end_date',
        'spell_end_date_truncated', 'spell_start_date_truncated'
    )
    .distinct()
    .withColumn(
        'days_in_hospital',
        f.when(
            f.col('spell_start_date_truncated') == f.col('date_of_death'),
            f.lit(0)
        )
        .when(
            f.col('spell_end_date_truncated') == f.col('spell_start_date_truncated'),
            f.lit(0.5)
        )
        .otherwise(
            f.datediff(f.col('spell_end_date_truncated'), f.col('spell_start_date_truncated'))
        )
    )
)

hes_apc_spells_consolidated = hes_apc_spells_consolidated.cache()

display(hes_apc_spells_consolidated)


# COMMAND ----------

hes_apc_days_in_hospital = (
    hes_apc_spells_consolidated
    .groupBy('row_id', 'person_id', 'index_stroke_date', 'date_of_death', 'follow_up_end_date')
    .agg(f.sum('days_in_hospital').alias('days_in_hospital'))
)

hes_apc_days_in_hospital = hes_apc_days_in_hospital.cache()

# COMMAND ----------


cohort_home_time = (
    cohort_selected
    .join(
        hes_apc_days_in_hospital
        .select('row_id', 'days_in_hospital'),
        how = 'left', on = 'row_id'
    )
    .na.fill({'days_in_hospital': 0})
    .withColumn(
        'days_dead',
        f.when(
            f.col('date_of_death') <= f.col('follow_up_end_date'),
            f.datediff(f.col('follow_up_end_date'), f.col('date_of_death'))
        )
        .otherwise(0)
    )
    .withColumn(
        'home_time',
        f.round(
            f.datediff(f.col('follow_up_end_date'), f.col('index_stroke_date'))
            - f.col('days_in_hospital')
            - f.col('days_dead'),
            2
        )
    )
    .select('row_id', 'person_id', 'days_in_hospital', 'days_dead', 'home_time')
)

save_table(cohort_home_time, table = 'cohort_home_time')

# Clear cache
spark.catalog.clearCache()

# COMMAND ----------

cohort_home_time = load_table('cohort_home_time')

display(
    cohort_home_time
    .agg(
        f.count('*'),
        f.min('home_time'),
        f.mean('home_time'),
        f.max('home_time'),
    )
)