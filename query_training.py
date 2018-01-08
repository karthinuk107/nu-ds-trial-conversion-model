# coding: utf-8

"""
data gathering for Trial Conversion model

"""
import yaml
import uuid
import time
import os
import argparse
from google.cloud import bigquery

parser = argparse.ArgumentParser()
parser.add_argument("env", help="The environment that you want to point to. "
                                "either 'dev', 'uat' or 'prod'")

### Daily Updation Queries

conversion_base_sql = open('queries/Daily_update/conversion_base_90days.sql', 'r').read()
omniture_history_sql= open('queries/Daily_update/omniture_history.sql', 'r').read()
axiom_features_sql = open('queries/Daily_update/axiom_scv_features.sql', 'r').read()
omniture_agg_sql = open('queries/Daily_update/omniture_agg.sql', 'r').read()

### Aggregation

## Training

# Mid Term Cancellation

trial_agg_31days_sql = open('queries/Mid_term_cancellation/Training/trial_agg_31days.sql', 'r').read()
page_views_trend_31days_sql = open('queries/Mid_term_cancellation/Training/page_views_trend_31days.sql', 'r').read()
trial_agg_page_views_31days_sql= open('queries/Mid_term_cancellation/Training/trial_agg_page_views_31days.sql', 'r').read()
trial_master_feature_31days_sql = open('queries/Mid_term_cancellation/Training/trial_master_feature_31days.sql', 'r').read()

# After Trial Cancellation

trial_agg_55days_sql = open('queries/After_trial_cancellation/Training/trial_agg_55days.sql', 'r').read()
page_views_trend_55days_sql = open('queries/After_trial_cancellation/Training/page_views_trend_55days.sql', 'r').read()
trial_agg_page_views_55days_sql= open('queries/After_trial_cancellation/Training/trial_agg_page_views_55days.sql', 'r').read()
trial_master_feature_55days_sql = open('queries/After_trial_cancellation/Training/trial_master_feature_55days.sql', 'r').read()

## Test

# Mid Term Cancellation

trial_agg_32to45days_sql = open('queries/Mid_term_cancellation/Test/trial_agg_32to45days.sql', 'r').read()
page_views_trend_32to45days_sql = open('queries/Mid_term_cancellation/Test/page_views_trend_32to45days.sql', 'r').read()
trial_agg_page_views_32to45days_sql = open('queries/Mid_term_cancellation/Test/trial_agg_page_views_32to45days.sql', 'r').read()
trial_master_feature_32to45days_sql  = open('queries/Mid_term_cancellation/Test/trial_master_feature_32to45days.sql', 'r').read()

# After Trial Cancellation

trial_agg_56to65days_sql = open('queries/After_trial_cancellation/Test/trial_agg_56to65days.sql', 'r').read()
page_views_trend_56to65days_sql = open('queries/After_trial_cancellation/Test/page_views_trend_56to65days.sql', 'r').read()
trial_agg_page_views_56to65days_sql = open('queries/After_trial_cancellation/Test/trial_agg_page_views_56to65days.sql', 'r').read()
trial_master_feature_56to65days_sql  = open('queries/After_trial_cancellation/Test/trial_master_feature_56to65days.sql', 'r').read()

## Final_Table

# Mid Term Cancellation

trial_agg_master_feature_31days_sql = open('queries/Mid_term_cancellation/Training/trial_agg_master_feature_31days.sql', 'r').read()
trial_agg_master_feature_32to45days_sql = open('queries/Mid_term_cancellation/Test/trial_agg_master_feature_32to45days.sql', 'r').read()

# After Trial Cancellation

trial_agg_master_feature_55days_sql = open('queries/After_trial_cancellation/Training/trial_agg_master_feature_55days.sql', 'r').read()
trial_agg_master_feature_56to65days_sql = open('queries/After_trial_cancellation/Test/trial_agg_master_feature_56to65days.sql', 'r').read()

def run_query_to_table_append(project, query, dataset, table):
    print "Updating Table: ", project,"-",dataset,"-",table
    dest_client = bigquery.Client(project=project)
    table = dest_client.dataset(dataset).table(table)
    source_client = bigquery.Client(project=project)
    query_job = source_client.run_async_query(str(uuid.uuid4()), query)
    query_job.allow_large_results = True
    query_job.destination = table
    query_job.use_legacy_sql = True
    query_job.priority = 'BATCH'
    query_job.createDisposition = "CREATE_IF_NEEDED"
    query_job.write_disposition = "WRITE_APPEND"
    query_job.use_cache="use_cache"
    # print query_job.query
    query_job.begin()

    wait_for_job(query_job)


def run_query_to_table_legacy(project, query, dataset, table):
    print "Updating Table: ", project,"-",dataset,"-",table
    dest_client = bigquery.Client(project=project)
    table = dest_client.dataset(dataset).table(table)
    source_client = bigquery.Client(project=project)
    query_job = source_client.run_async_query(str(uuid.uuid4()), query)
    query_job.allow_large_results = True
    query_job.destination = table
    query_job.use_legacy_sql = True
    query_job.priority = 'BATCH'
    query_job.createDisposition = "CREATE_IF_NEEDED"
    query_job.write_disposition = "WRITE_TRUNCATE"
    query_job.use_cache="use_cache"
    query_job.begin()

    wait_for_job(query_job)


def run_query_to_table_standard(project, query, dataset, table):
    print "Updating Table: ", project,"-",dataset,"-",table
    dest_client = bigquery.Client(project=project)
    table = dest_client.dataset(dataset).table(table)
    source_client = bigquery.Client(project=project)
    query_job = source_client.run_async_query(str(uuid.uuid4()), query)
    query_job.allow_large_results = True
    query_job.destination = table
    query_job.use_legacy_sql = False
    query_job.priority = 'BATCH'
    query_job.write_disposition = "WRITE_TRUNCATE"
    query_job.createDisposition = "CREATE_IF_NEEDED"
    query_job.use_cache="use_cache"
    # print query_job.query
    query_job.begin()

    wait_for_job(query_job)

def wait_for_job(job):
    while True:
        job.reload()
        if job.state == 'DONE':
            if job.error_result:
                raise RuntimeError(job.errors)
            return
        time.sleep(1)


def conversion_features_to_bq(yml_config_filename):

    """
    Queries to extract trial features data and post this to a table in Google
    BigQuery.
    """

    # .....................................
    # GET PARAMETERS
    # .....................................

    # Get parameters from YAML config file

    with open(yml_config_filename, 'r') as ymlfile:
        cfg = yaml.load(ymlfile)

    project = str(cfg["project"])

    # BQ Dataset Names

    dataset = cfg["input_table"]
    dataset_mid = cfg["input_table_mid_term"]
    dataset_after = cfg["input_table_after_trial"]

    ### BQ Table Names

    conversion_base = cfg["conversion_base"]
    omniture_history = cfg["omniture_history"]
    omniture_agg = cfg["omniture_agg"]
    axiom_features = cfg["axiom_features"]

    ### Mid Term Cancellation

    # Training

    trial_agg_31days = cfg["trial_agg_31days"]
    page_views_trend_31days = cfg["page_views_trend_31days"]
    trial_agg_page_views_31days = cfg["trial_agg_page_views_31days"]
    trial_master_feature_31days = cfg["trial_master_feature_31days"]
    trial_agg_master_feature_31days = cfg["trial_agg_master_feature_31days"]

    # Test

    trial_agg_32to45days = cfg["trial_agg_32to45days"]
    page_views_trend_32to45days = cfg["page_views_trend_32to45days"]
    trial_agg_page_views_32to45days = cfg["trial_agg_page_views_32to45days"]
    trial_master_feature_32to45days = cfg["trial_master_feature_32to45days"]
    trial_agg_master_feature_32to45days = cfg["trial_agg_master_feature_32to45days"]

    ### After Trial Cancellation

    trial_agg_55days = cfg["trial_agg_55days"]
    page_views_trend_55days = cfg["page_views_trend_55days"]
    trial_agg_page_views_55days = cfg["trial_agg_page_views_55days"]
    trial_master_feature_55days = cfg["trial_master_feature_55days"]
    trial_agg_master_feature_55days = cfg["trial_agg_master_feature_55days"]

    # Test

    trial_agg_56to65days = cfg["trial_agg_56to65days"]
    page_views_trend_56to65days = cfg["page_views_trend_56to65days"]
    trial_agg_page_views_56to65days = cfg["trial_agg_page_views_56to65days"]
    trial_master_feature_56to65days = cfg["trial_master_feature_56to65days"]
    trial_agg_master_feature_56to65days = cfg["trial_agg_master_feature_56to65days"]

    # .....................................
    # Query and Update Table
    # .....................................

    # Query for Base Table Update

    run_query_to_table_append(project,conversion_base_sql,dataset,conversion_base)
    run_query_to_table_append(project,omniture_history_sql.format(project,dataset,conversion_base),dataset,omniture_history)
    run_query_to_table_standard(project,omniture_agg_sql.format(project,dataset,conversion_base,omniture_history),dataset,omniture_agg)
    run_query_to_table_legacy(project,axiom_features_sql,dataset,axiom_features)

    # Queries for Training (From Trail Start to 31 days of activity and other Features)

    run_query_to_table_legacy(project,trial_agg_31days_sql.format(project,dataset,omniture_agg),dataset_mid,trial_agg_31days)
    run_query_to_table_legacy(project,page_views_trend_31days_sql.format(project,dataset,omniture_agg),dataset_mid,page_views_trend_31days)
    run_query_to_table_standard(project,trial_agg_page_views_31days_sql.format(project,dataset_mid,trial_agg_31days,page_views_trend_31days),dataset_mid,trial_agg_page_views_31days)
    run_query_to_table_standard(project,trial_master_feature_31days_sql.format(project,dataset_mid,dataset,trial_agg_page_views_31days,axiom_features),dataset_mid,trial_master_feature_31days)
    run_query_to_table_legacy(project,trial_agg_master_feature_31days_sql.format(project,dataset_mid,trial_master_feature_31days),dataset_mid,trial_agg_master_feature_31days)

    # Queries for Test (Trail Start between 32 to 45 days of activity and other Features)

    run_query_to_table_legacy(project,trial_agg_32to45days_sql.format(project,dataset,omniture_agg),dataset_mid,trial_agg_32to45days)
    run_query_to_table_legacy(project,page_views_trend_32to45days_sql.format(project,dataset,omniture_agg),dataset_mid,page_views_trend_32to45days)
    run_query_to_table_standard(project,trial_agg_page_views_32to45days_sql.format(project,dataset_mid,trial_agg_32to45days,page_views_trend_32to45days),dataset_mid,trial_agg_page_views_32to45days)
    run_query_to_table_standard(project,trial_master_feature_32to45days_sql.format(project,dataset_mid,dataset,trial_agg_page_views_32to45days,axiom_features),dataset_mid,trial_master_feature_32to45days)
    run_query_to_table_legacy(project,trial_agg_master_feature_32to45days_sql.format(project,dataset_mid,trial_master_feature_32to45days),dataset_mid,trial_agg_master_feature_32to45days)

    # Queries for Training (From Trail Start to 55 days of activity and other Features)

    run_query_to_table_legacy(project,trial_agg_55days_sql.format(project,dataset,omniture_agg),dataset_after,trial_agg_55days)
    run_query_to_table_legacy(project,page_views_trend_55days_sql.format(project,dataset,omniture_agg),dataset_after,page_views_trend_55days)
    run_query_to_table_standard(project,trial_agg_page_views_55days_sql.format(project,dataset_after,trial_agg_55days,page_views_trend_55days),dataset_after,trial_agg_page_views_55days)
    run_query_to_table_standard(project,trial_master_feature_55days_sql.format(project,dataset_after,dataset,trial_agg_page_views_55days,axiom_features),dataset_after,trial_master_feature_55days)
    run_query_to_table_legacy(project,trial_agg_master_feature_55days_sql.format(project,dataset_after,trial_master_feature_55days),dataset_after,trial_agg_master_feature_55days)

    # Queries for Test (Trail Start between 56 to 65 days of activity and other Features)

    run_query_to_table_legacy(project,trial_agg_56to65days_sql.format(project,dataset,omniture_agg),dataset_after,trial_agg_56to65days)
    run_query_to_table_legacy(project,page_views_trend_56to65days_sql.format(project,dataset,omniture_agg),dataset_after,page_views_trend_56to65days)
    run_query_to_table_standard(project,trial_agg_page_views_56to65days_sql.format(project,dataset_after,trial_agg_56to65days,page_views_trend_56to65days),dataset_after,trial_agg_page_views_56to65days)
    run_query_to_table_standard(project,trial_master_feature_56to65days_sql.format(project,dataset_after,dataset,trial_agg_page_views_56to65days,axiom_features),dataset_after,trial_master_feature_56to65days)
    run_query_to_table_legacy(project,trial_agg_master_feature_56to65days_sql.format(project,dataset_after,trial_master_feature_56to65days),dataset_after,trial_agg_master_feature_56to65days)

if __name__ == "__main__":
    args = parser.parse_args()
    config_file = os.path.join(os.path.dirname(__file__), "config/config_{}.yaml".format(args.env))
    conversion_features_to_bq(config_file)