from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator

from dec.operators import CheckConfigurationOperator, RedshiftStagingOperator, \
    DataQualityCheckAfterStreamingOperator, CreateDimensionOperator, CreateFactOperator, \
    LoadDimensionOperator, LoadFactOperator, DataQualityCheckDBOperator
from dec.config import config
from dec.sql import create_raw_teams_table, create_raw_shots_table, \
    create_raw_players_2_table, create_raw_players_1_table, create_raw_all_star_table
from dec.sql import create_dimension_all_star_games, create_dimension_historical, \
    create_dimension_player, create_fact_shots
from dec.sql import copy_raw_table
from dec.sql import insert_dim_all_star, insert_dim_historical, insert_dim_players, insert_fact_shots

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
    skip=config.get("skip_stage_3"),
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
    skip=config.get("skip_stage_3"),
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
    skip=config.get("skip_stage_3"),
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
    skip=config.get("skip_stage_3"),
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
    skip=config.get("skip_stage_3"),
    arn=config.get("redshift_arn")
)

csvs = [config.get("s3_historical_key"), config.get("s3_all_star_key"),
        config.get("s3_players_2_key"), config.get("s3_players_1_key"),
        config.get("s3_shots_key")]

tables = ["raw_historical_table", "raw_allstar_table", "raw_players_2_table",
          "raw_players_1_table", "raw_shots_table"]

step_data_quality_check_1 = DataQualityCheckAfterStreamingOperator(
    dag=dag,
    task_id="step_data_quality_check_1",
    db_conn_id=config.get("redshift_conn_id"),
    aws_credentials_id=config.get("aws_credentials_id"),
    s3_bucket=config.get("s3_bucket"),
    csvs=csvs,
    tables=tables,
    skip=config.get("skip_stage_4")
)

step_create_dimension_player = CreateDimensionOperator(
    dag=dag,
    task_id="create_dimension_player",
    db_conn_id=config.get("redshift_conn_id"),
    dimension="dimension_player",
    dimension_query=create_dimension_player,
    skip=config.get("skip_stage_5")
)

step_create_dimension_teams = CreateDimensionOperator(
    dag=dag,
    task_id="create_dimension_teams",
    db_conn_id=config.get("redshift_conn_id"),
    dimension="dimension_historical",
    dimension_query=create_dimension_historical,
    skip=config.get("skip_stage_5")
)

step_create_dimension_all_star = CreateDimensionOperator(
    dag=dag,
    task_id="create_dimension_all_star",
    db_conn_id=config.get("redshift_conn_id"),
    dimension="dimension_all_star",
    dimension_query=create_dimension_all_star_games,
    skip=config.get("skip_stage_5")
)

step_create_fact_shots = CreateFactOperator(
    dag=dag,
    task_id="create_fact_shots",
    db_conn_id=config.get("redshift_conn_id"),
    fact="fact_shots",
    historical_table="dimension_historical",
    all_star_table="dimension_all_star",
    player_table="dimension_player",
    fact_query=create_fact_shots,
    skip=config.get("skip_stage_5")
)

step_load_dimension_player = LoadDimensionOperator(
    dag=dag,
    task_id="load_dim_player",
    db_conn_id=config.get("redshift_conn_id"),
    table="dimension_player",
    raw_table1="raw_players_1_table",
    raw_table2="raw_players_2_table",
    insert_query=insert_dim_players,
    skip=config.get("skip_stage_6")
)

step_load_dimension_historical = LoadDimensionOperator(
    dag=dag,
    task_id="load_dim_historical",
    db_conn_id=config.get("redshift_conn_id"),
    table="dimension_historical",
    raw_table1="raw_historical_table",
    raw_table2="",
    insert_query=insert_dim_historical,
    skip=config.get("skip_stage_6")
)

step_load_dimension_all_star = LoadDimensionOperator(
    dag=dag,
    task_id="load_dim_all_star",
    db_conn_id=config.get("redshift_conn_id"),
    table="dimension_all_star",
    raw_table1="raw_allstar_table",
    raw_table2="",
    insert_query=insert_dim_all_star,
    skip=config.get("skip_stage_6")
)

step_load_fact_shots = LoadFactOperator(
    dag=dag,
    task_id="load_fact_shots",
    db_conn_id=config.get("redshift_conn_id"),
    table="fact_shots",
    raw_table="raw_shots_table",
    dim_player="dimension_player",
    dim_historical="dimension_historical",
    dim_all_star="dimension_all_star",
    insert_query=insert_fact_shots,
    skip=config.get("skip_stage_6")
)

step_data_quality_check_2 = DataQualityCheckDBOperator(
    dag=dag,
    task_id="step_data_quality_check_2",
    db_conn_id=config.get("redshift_conn_id"),
    tables=[("dimension_player", "raw_players_1_table", "raw_players_2_table"),
            ("dimension_all_star", "raw_allstar_table"),
            ("dimension_historical", "raw_historical_table"),
            ("fact_shots", "raw_shots_table")],
    skip=config.get("skip_stage_7")
)


step_end_exec = DummyOperator(
    task_id="end",
    dag=dag
)


# Execution order
step_begin_exec >> step_check_config >> \
    [step_stage_table_allstar_to_redshift,
     step_stage_table_shots_to_redshift,
     step_stage_table_historical_to_redshift,
     step_stage_table_players_1_to_redshift,
     step_stage_table_players_2_to_redshift] >> step_data_quality_check_1 >> \
    [step_create_dimension_player,
     step_create_dimension_all_star,
     step_create_dimension_teams] >> step_create_fact_shots >>  \
    [step_load_dimension_all_star,
     step_load_dimension_historical,
     step_load_dimension_player] >> step_load_fact_shots >> step_data_quality_check_2 >> step_end_exec
# step_begin_exec >> step_check_config >> step_stage_table_shots_to_redshift >> step_end_exec
# step_begin_exec >> step_check_config >> step_stage_table_players_1_to_redshift >> step_end_exec
# step_begin_exec >> step_check_config >> step_stage_table_players_2_to_redshift >> step_end_exec
# step_begin_exec >> step_check_config >> step_stage_table_allstar_to_redshift >> step_end_exec
# step_begin_exec >> step_check_config >> step_stage_table_historical_to_redshift >> step_end_exec
