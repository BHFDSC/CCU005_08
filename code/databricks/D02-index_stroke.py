# Databricks notebook source
# MAGIC %run ./project_config

# COMMAND ----------

# MAGIC %run ./D00-parameters

# COMMAND ----------

from hds_functions import load_table, save_table, map_column_values, first_row
from pyspark.sql import functions as f, Window

# COMMAND ----------

stroke_events = load_table('stroke_events')

index_stroke_records = (
    stroke_events
    .withColumn('min_stroke_date', f.to_date(f.lit(min_stroke_date)))
    .withColumn('max_stroke_date', f.to_date(f.lit(max_stroke_date)))
    .withColumn('study_start_date', f.to_date(f.lit(study_start_date)))
    .withColumn('study_end_date', f.to_date(f.lit(study_end_date)))
)

# COMMAND ----------

_win_id = (
    Window
    .partitionBy('person_id')
)

_win_id_data_source = (
    Window
    .partitionBy('person_id', 'data_source')
)

_win_id_ordered_unbounded = (
    Window
    .partitionBy('person_id')
    .orderBy('date', 'data_source', 'code')
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
)

# COMMAND ----------


index_stroke_records = (
    index_stroke_records
    .withColumn(
        'index_stroke_date',
        f.min('date').over(_win_id)
    )
    .withColumn(
        'min_stroke_date_within_source',
        f.min('date').over(_win_id_data_source)
    )
    .withColumn(
        'within_30_days_of_index_date',
        f.when(
            f.col('date') <= f.date_add('index_stroke_date', 30),
            f.lit(1)
        )
    )
    .withColumn(
        'linked_index_stroke',
        f.when(
            (f.col('within_30_days_of_index_date') == f.lit(1))
            & (f.col('data_source').isin(['gdppr', 'deaths'])),
            f.lit(1)
        )
        .when(
            (f.col('within_30_days_of_index_date') == f.lit(1))
            & (f.col('data_source').isin(['ssnap', 'hes_apc']))
            & (f.col('date') == f.col('min_stroke_date_within_source')),
            f.lit(1)
        )
    )
)

index_stroke_records = (
    index_stroke_records
    .filter("linked_index_stroke = 1")
    .filter("(index_stroke_date >= min_stroke_date) AND (index_stroke_date <= max_stroke_date)")
)

index_stroke_records = (
    index_stroke_records
    .withColumn(
        'stroke_ssnap',
        f.max(
            f.when(
                (f.col('data_source') == f.lit('ssnap')),
                f.lit(1)
            )
        ).over(_win_id)
    )
    .withColumn(
        'stroke_hes_apc',
        f.max(
            f.when(
                (f.col('data_source') == f.lit('hes_apc')),
                f.lit(1)
            )
        ).over(_win_id)
    )
    .withColumn(
        'stroke_gdppr',
        f.max(
            f.when(
                (f.col('data_source') == f.lit('gdppr')),
                f.lit(1)
            )
        ).over(_win_id)
    )
      .withColumn(
        'stroke_deaths',
        f.max(
            f.when(
                (f.col('data_source') == f.lit('deaths')),
                f.lit(1)
            )
        ).over(_win_id)
    )
    .withColumn(
        'ssnap_ischaemic',
        f.max(
            f.when(
                (f.col('data_source') == f.lit('ssnap'))
                & (f.col('stroke_type') == f.lit('ischaemic')),
                f.lit(1)
            )
        ).over(_win_id)
    )
    .withColumn(
        'ssnap_haemorrhagic',
        f.max(
            f.when(
                (f.col('data_source') == f.lit('ssnap'))
                & (f.col('stroke_type') == f.lit('haemorrhagic')),
                f.lit(1)
            )
        ).over(_win_id)
    )
    .withColumn(
        'ssnap_missing',
        f.max(
            f.when(
                (f.col('data_source') == f.lit('ssnap'))
                & (f.col('stroke_type') == f.lit('missing')),
                f.lit(1)
            )
        ).over(_win_id)
    )
    .withColumn(
        'hes_apc_ischaemic',
        f.max(
            f.when(
                (f.col('data_source') == f.lit('hes_apc'))
                & (f.col('stroke_type') == f.lit('ischaemic')),
                f.lit(1)
            )
        ).over(_win_id)
    )
    .withColumn(
        'hes_apc_haemorrhagic',
        f.max(
            f.when(
                (f.col('data_source') == f.lit('hes_apc'))
                & (f.col('stroke_type') == f.lit('haemorrhagic')),
                f.lit(1)
            )
        ).over(_win_id)
    )
    .withColumn(
        'hes_apc_nos',
        f.max(
            f.when(
                (f.col('data_source') == f.lit('hes_apc'))
                & (f.col('stroke_type') == f.lit('nos')),
                f.lit(1)
            )
        ).over(_win_id)
    )
    .withColumn(
        'gdppr_ischaemic',
        f.max(
            f.when(
                (f.col('data_source') == f.lit('gdppr'))
                & (f.col('stroke_type') == f.lit('ischaemic')),
                f.lit(1)
            )
        ).over(_win_id)
    )
    .withColumn(
        'gdppr_haemorrhagic',
        f.max(
            f.when(
                (f.col('data_source') == f.lit('gdppr'))
                & (f.col('stroke_type') == f.lit('haemorrhagic')),
                f.lit(1)
            )
        ).over(_win_id)
    )
    .withColumn(
        'gdppr_nos',
        f.max(
            f.when(
                (f.col('data_source') == f.lit('gdppr'))
                & (f.col('stroke_type') == f.lit('nos')),
                f.lit(1)
            )
        ).over(_win_id)
    )
    .withColumn(
        'deaths_ischaemic',
        f.max(
            f.when(
                (f.col('data_source') == f.lit('deaths'))
                & (f.col('stroke_type') == f.lit('ischaemic')),
                f.lit(1)
            )
        ).over(_win_id)
    )
    .withColumn(
        'deaths_haemorrhagic',
        f.max(
            f.when(
                (f.col('data_source') == f.lit('deaths'))
                & (f.col('stroke_type') == f.lit('haemorrhagic')),
                f.lit(1)
            )
        ).over(_win_id)
    )
    .withColumn(
        'deaths_nos',
        f.max(
            f.when(
                (f.col('data_source') == f.lit('deaths'))
                & (f.col('stroke_type') == f.lit('nos')),
                f.lit(1)
            )
        ).over(_win_id)
    )
)

index_stroke_records = (
    index_stroke_records
    .withColumn(
        'stroke_date_list',
        f.collect_list('date').over(_win_id_ordered_unbounded)
    )
    .withColumn(
        'stroke_code_list',
        f.collect_list('code').over(_win_id_ordered_unbounded)
    )
    .withColumn(
        'stroke_description_list',
        f.collect_list('description').over(_win_id_ordered_unbounded)
    )
    .withColumn(
        'stroke_source_list',
        f.collect_list('data_source').over(_win_id_ordered_unbounded)
    )
    .withColumn(
        'stroke_type_list',
        f.collect_list('stroke_type').over(_win_id_ordered_unbounded)
    )
)


# COMMAND ----------

index_stroke_records = (
    index_stroke_records

    # SSNAP
    .withColumn(
        'stroke_type_harmonised',
        f.when(
            (f.col('ssnap_ischaemic') == f.lit(1))
            & (f.col('ssnap_haemorrhagic').isNull()),
            f.lit('Ischaemic')
        )
        .when(
            (f.col('ssnap_ischaemic').isNull())
            & (f.col('ssnap_haemorrhagic') == f.lit(1)),
            f.lit('Haemorrhagic')
        )
        .when(
            (f.col('ssnap_ischaemic') == f.lit(1))
            & (f.col('ssnap_haemorrhagic') == f.lit(1)),
            f.lit('Unknown')
        )

        # HES-APC: classified
        .when(
            (f.col('hes_apc_ischaemic') == f.lit(1))
            & (f.col('hes_apc_haemorrhagic').isNull()),
            f.lit('Ischaemic')
        )
        .when(
            (f.col('hes_apc_ischaemic').isNull())
            & (f.col('hes_apc_haemorrhagic') == f.lit(1)),
            f.lit('Haemorrhagic')
        )

        # HES-APC: Mixed
        .when(
            (f.col('hes_apc_ischaemic') == f.lit(1))
            & (f.col('hes_apc_haemorrhagic') == f.lit(1)),
            f.lit('Unknown')
        )

        # GDPPR: classified
        .when(
            (f.col('gdppr_ischaemic') == f.lit(1))
            & (f.col('gdppr_haemorrhagic').isNull()),
            f.lit('Ischaemic')
        )
        .when(
            (f.col('gdppr_ischaemic').isNull())
            & (f.col('gdppr_haemorrhagic') == f.lit(1)),
            f.lit('Haemorrhagic')
        )

        # GDPPR: Mixed 
        .when(
            (f.col('gdppr_ischaemic') == f.lit(1))
            & (f.col('gdppr_haemorrhagic') == f.lit(1)),
            f.lit('Unknown')
        )

        # Deaths: classified
        .when(
            (f.col('deaths_ischaemic') == f.lit(1))
            & (f.col('deaths_haemorrhagic').isNull()),
            f.lit('Ischaemic')
        )
        .when(
            (f.col('deaths_ischaemic').isNull())
            & (f.col('deaths_haemorrhagic') == f.lit(1)),
            f.lit('Haemorrhagic')
        )

        # Deaths: Mixed 
        .when(
            (f.col('deaths_ischaemic') == f.lit(1))
            & (f.col('deaths_haemorrhagic') == f.lit(1)),
            f.lit('Unknown')
        )

        .otherwise('Unknown')
    )
)

save_table(df = index_stroke_records, table = 'index_stroke_records')

# COMMAND ----------

index_stroke_records = load_table('index_stroke_records')

index_stroke = (
    index_stroke_records
    .transform(
        first_row, n = 1,
        partition_by = ['person_id'],
        order_by = [f.lit(1)]
    )
)

index_stroke = (
    index_stroke
    .drop(*['date', 'code', 'description', 'stroke_type', 'data_source'])
)

save_table(df = index_stroke, table = 'index_stroke')

# COMMAND ----------

index_stroke = load_table('index_stroke')

display(index_stroke)