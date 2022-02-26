from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator

from dec.operators import CheckConfigurationOperator, RedshiftStagingOperator
from dec.config import config
from dec.sql import create_raw_teams_table, create_raw_shots_table, \
    create_raw_players_2_table, create_raw_players_1_table, create_raw_all_star_table
from dec.sql import copy_raw_table

import datetime
from configparser import ConfigParser
import os


default_args = {
    'owner': 'Luis Alfredo Leon',
    'depends_on_past': False,
    'start_date': datetime.datetime.now(),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
    'catchup': False
}

dag = DAG("nba",
          max_active_runs=1,
          default_args=default_args)

step_begin_exec = DummyOperator(task_id="Begin",
                                dag=dag)

step_check_config = CheckConfigurationOperator(
    dag=dag,
    config=config,
    task_id="config_check"
)

step_stage_table_shots_to_redshift = RedshiftStagingOperator(
    dag=dag,
    task_id="stage_table_shots_to_redshift",
    db_conn_id=config.get("redshift_conn_id"),
    aws_credentials_id=config.get("aws_credentials_id"),
    table="raw_shots_table",
    s3_bucket=config.get("s3_bucket"),
    s3_key=config.get("s3_shots_key"),
    create_query=create_raw_shots_table,
    copy_query=copy_raw_table,
    clean=False,
    skip=False,
    arn=config.get("redshift_arn")
)

step_stage_table_players_1_to_redshift = RedshiftStagingOperator(
    dag=dag,
    task_id="stage_table_players_1_to_redshift",
    db_conn_id=config.get("redshift_conn_id"),
    aws_credentials_id=config.get("aws_credentials_id"),
    table="raw_players_1_table",
    s3_bucket=config.get("s3_bucket"),
    s3_key=config.get("s3_players_1_key"),
    create_query=create_raw_players_1_table,
    copy_query=copy_raw_table,
    clean=False,
    skip=False,
    arn=config.get("redshift_arn")
)

step_stage_table_players_2_to_redshift = RedshiftStagingOperator(
    dag=dag,
    task_id="stage_table_players_2_to_redshift",
    db_conn_id=config.get("redshift_conn_id"),
    aws_credentials_id=config.get("aws_credentials_id"),
    table="raw_players_2_table",
    s3_bucket=config.get("s3_bucket"),
    s3_key=config.get("s3_players_2_key"),
    create_query=create_raw_players_2_table,
    copy_query=copy_raw_table,
    clean=False,
    skip=False,
    arn=config.get("redshift_arn")
)

step_stage_table_allstar_to_redshift = RedshiftStagingOperator(
    dag=dag,
    task_id="stage_table_allstar_to_redshift",
    db_conn_id=config.get("redshift_conn_id"),
    aws_credentials_id=config.get("aws_credentials_id"),
    table="raw_allstar_table",
    s3_bucket=config.get("s3_bucket"),
    s3_key=config.get("s3_all_star_key"),
    create_query=create_raw_all_star_table,
    copy_query=copy_raw_table,
    clean=False,
    skip=False,
    arn=config.get("redshift_arn")
)

step_stage_table_historical_to_redshift = RedshiftStagingOperator(
    dag=dag,
    task_id="stage_table_historical_to_redshift",
    db_conn_id=config.get("redshift_conn_id"),
    aws_credentials_id=config.get("aws_credentials_id"),
    table="raw_historical_table",
    s3_bucket=config.get("s3_bucket"),
    s3_key=config.get("s3_historical_key"),
    create_query=create_raw_teams_table,
    copy_query=copy_raw_table,
    clean=False,
    skip=False,
    arn=config.get("redshift_arn")
)

step_end_exec = DummyOperator(task_id="end",
                                dag=dag)


# Execution order
step_begin_exec >> step_check_config >> step_stage_table_shots_to_redshift >> step_end_exec
step_begin_exec >> step_check_config >> step_stage_table_players_1_to_redshift >> step_end_exec
step_begin_exec >> step_check_config >> step_stage_table_players_2_to_redshift >> step_end_exec
step_begin_exec >> step_check_config >> step_stage_table_allstar_to_redshift >> step_end_exec
step_begin_exec >> step_check_config >> step_stage_table_historical_to_redshift >> step_end_exec
