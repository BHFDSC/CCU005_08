# Databricks notebook source
# MAGIC %run ./project_config

# COMMAND ----------

import json
from hds_functions import write_json_file

# Database names
db = 'dars_nic_391419_j3w9t'
dbc = f'{db}_collab'
dsa = 'dsa_391419_j3w9t_collab'
dss = 'dss_corporate'

# Table directory
archive_date = '2024-10-24'

archive_date_underscore = archive_date.replace("-", "_")

table_directory = {
    # --------------------------------------------------------------------------
    # Provisioned datasets
    # --------------------------------------------------------------------------
    'gdppr': {
        'database': dbc,
        'table_name': 'gdppr_dars_nic_391419_j3w9t_archive',
        'archive_date': archive_date
    },
    'hes_apc': {
        'database': dbc,
        'table_name': 'hes_apc_all_years_archive',
        'archive_date': archive_date
    },
    'ssnap': {
        'database': dbc,
        'table_name': 'ssnap_dars_nic_391419_j3w9t_archive',
        'archive_date': archive_date
    },
    'vaccine_status':{
        'database': dbc,
        'table_name': 'vaccine_status_dars_nic_391419_j3w9t_archive',
        'archive_date': archive_date
    },
    'deaths': {
        'database': dbc,
        'table_name': 'deaths_dars_nic_391419_j3w9t_archive',
        'archive_date': archive_date
    },
    'primary_care_meds':{
        'database': dbc,
        'table_name': 'primary_care_meds_dars_nic_391419_j3w9t_archive',
        'archive_date': archive_date
    },
    'token_pseudo_id_lookup':{
        'database': dbc,
        'table_name': 'token_pseudo_id_lookup_archive',
        'archive_date': archive_date
    },
    # --------------------------------------------------------------------------
    # HDS Common Tables - Demographics
    # --------------------------------------------------------------------------
    'gdppr_demographics_all_versions': {
        'database': dsa,
        'table_name': f'hds_curated_assets__gdppr_demographics',
    },
    'gdppr_demographics': {
        'database': dsa,
        'table_name': f'hds_curated_assets__gdppr_demographics',
        'max_archive_date': archive_date
    },
    'date_of_birth_multisource': {
        'database': dsa,
        'table_name': f'hds_curated_assets__date_of_birth_multisource_{archive_date_underscore}',
    },
    'date_of_birth_individual': {
        'database': dsa,
        'table_name': f'hds_curated_assets__date_of_birth_individual_{archive_date_underscore}',
    },
    'sex_multisource': {
        'database': dsa,
        'table_name': f'hds_curated_assets__sex_multisource_{archive_date_underscore}',
    },
    'sex_individual': {
        'database': dsa,
        'table_name': f'hds_curated_assets__sex_individual_{archive_date_underscore}',
    },
    'ethnicity_multisource': {
        'database': dsa,
        'table_name': f'hds_curated_assets__ethnicity_multisource_{archive_date_underscore}',
    },
    'ethnicity_individual': {
        'database': dsa,
        'table_name': f'hds_curated_assets__ethnicity_individual_{archive_date_underscore}',
    },
    'lsoa_multisource': {
        'database': dsa,
        'table_name': f'hds_curated_assets__lsoa_multisource_{archive_date_underscore}',
    },
    'lsoa_individual': {
        'database': dsa,
        'table_name': f'hds_curated_assets__lsoa_individual_{archive_date_underscore}',
    },
    'demographics': {
        'database': dsa,
        'table_name': f'hds_curated_assets__demographics_{archive_date_underscore}',
    },
    # --------------------------------------------------------------------------
    # HDS Common Tables - HES-APC 
    # --------------------------------------------------------------------------
    'hes_apc_diagnosis': {
        'database': dsa,
        'table_name': f'hds_curated_assets__hes_apc_diagnosis_{archive_date_underscore}',
    },
    'hes_apc_procedure': {
        'database': dsa,
        'table_name': f'hds_curated_assets__hes_apc_procedure_{archive_date_underscore}',
    },
    'hes_apc_cips_episodes': {
        'database': dsa,
        'table_name': f'hds_curated_assets__hes_apc_cips_episodes_{archive_date_underscore}',
    },
    'hes_apc_cips_provider_spells': {
        'database': dsa,
        'table_name': f'hds_curated_assets__hes_apc_cips_provider_spells_{archive_date_underscore}',
    },
    'hes_apc_cips_cips': {
        'database': dsa,
        'table_name': f'hds_curated_assets__hes_apc_cips_cips_{archive_date_underscore}',
    },
    # --------------------------------------------------------------------------
    # HDS Common Tables - ONS Deaths
    # --------------------------------------------------------------------------
    'deaths_single': {
        'database': dsa,
        'table_name': f'hds_curated_assets__deaths_single_{archive_date_underscore}',
    },
    'deaths_cause_of_death': {
        'database': dsa,
        'table_name': f'hds_curated_assets__deaths_cause_of_death_{archive_date_underscore}',
    },
    # --------------------------------------------------------------------------
    # HDS Common Tables - Covid-19
    # --------------------------------------------------------------------------
    'covid_positive': {
        'database': dsa,
        'table_name': f'hds_curated_assets__covid_positive_{archive_date_underscore}',
    },
    # --------------------------------------------------------------------------
    # Project tables
    # --------------------------------------------------------------------------

    # D01-stroke_events

    'stroke_events': {
        'database': dsa,
        'table_name': f'{project_name}__stroke_events',
    },

    # D02-index_stroke

    'index_stroke_records': {
        'database': dsa,
        'table_name': f'{project_name}__index_stroke_records',
    },

    'index_stroke': {
        'database': dsa,
        'table_name': f'{project_name}__index_stroke',
    },

    # D03-cohort_pre_exclusions

    'cohort_pre_exclusions': {
        'database': dsa,
        'table_name': f'{project_name}__cohort_pre_exclusions',
    },

    # D04-cohort_filtered

    'cohort': {
        'database': dsa,
        'table_name': f'{project_name}__cohort',
    },
    
    'cohort_inclusion_flowchart': {
        'database': dsa,
        'table_name': f'{project_name}__cohort_inclusion_flowchart',
    },

    # D05-region_imd

    'cohort_region_imd': {
        'database': dsa,
        'table_name': f'{project_name}__cohort_region_imd',
    },

    # D06-home_time

    'cohort_home_time': {
        'database': dsa,
        'table_name': f'{project_name}__cohort_home_time',
    },

    # D07-covid_positive

    'cohort_covid_positive': {
        'database': dsa,
        'table_name': f'{project_name}__cohort_covid_positive',
    },

    # D08-measures

    'cohort_measures': {
        'database': dsa,
        'table_name': f'{project_name}__cohort_measures',
    },

    # D09-comorbidities

    'cohort_comorbidities': {
        'database': dsa,
        'table_name': f'{project_name}__cohort_comorbidities',
    },

    # D10-medications_pmeds

    'cohort_primary_care_meds_full': {
        'database': dsa,
        'table_name': f'{project_name}__cohort_primary_care_meds_full',
    },

    'cohort_primary_care_meds_first_code_ps': {
        'database': dsa,
        'table_name': f'{project_name}__cohort_primary_care_meds_first_code_ps',
    },

    'cohort_prior_medications_pmed': {
        'database': dsa,
        'table_name': f'{project_name}__cohort_prior_medications_pmed',
    },

    'cohort_ps_medications_pmed': {
        'database': dsa,
        'table_name': f'{project_name}__cohort_ps_medications_pmed',
    },

    # D11-medications_gdppr

    'cohort_gdppr_meds_full': {
        'database': dsa,
        'table_name': f'{project_name}__cohort_gdppr_meds_full',
    },

    'cohort_prior_medications_gdppr': {
        'database': dsa,
        'table_name': f'{project_name}__cohort_prior_medications_gdppr',
    },

    'cohort_ps_medications_gdppr': {
        'database': dsa,
        'table_name': f'{project_name}__cohort_ps_medications_gdppr',
    },

    # D12-ssnap_variables

    'cohort_ssnap_variables': {
        'database': dsa,
        'table_name': f'{project_name}__cohort_ssnap_variables',
    },

    # D13-cause_of_death

    'cohort_cause_of_death': {
        'database': dsa,
        'table_name': f'{project_name}__cohort_cause_of_death',
    },

    # D14-smoking

    'cohort_smoking': {
        'database': dsa,
        'table_name': f'{project_name}__cohort_smoking',
    },

    # D15-combine_tables 

    'cohort_final': {
        'database': dsa,
        'table_name': f'{project_name}__cohort_final',
    }
}

# Write table_directory
write_json_file(table_directory, path = './config/table_directory.json')