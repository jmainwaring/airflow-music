from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator,
                                PostgresOperator)
from helpers import SqlQueries, DataQualityChecks


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
        )

start_operator = DummyOperator(task_id = 'Begin_execution',  dag = dag)

create_tables_task = PostgresOperator(
  task_id =  'create_tables',
  dag = dag,
  sql = 'create_tables.sql',
  postgres_conn_id = 'redshift'
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id ='Stage_events',
    dag = dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    staging_table = 'staging_events',
    s3_bucket = 'udacity-dend',
    s3_directory = 'log_data',
    json_formatting = 's3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id ='Stage_songs',
    dag = dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    staging_table = 'staging_songs',
    s3_bucket = 'udacity-dend',
    s3_directory = 'song_data/A/A/A',
)


# Loading fact/dimension tables 
load_songplays_table = LoadFactOperator(
    task_id = 'Load_songplays_fact_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    append_mode = False,
    fct_table = 'songplays'
)

load_user_dimension_table = LoadDimensionOperator(
    task_id = 'Load_user_dim_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    dim_table = 'users'
)

load_song_dimension_table = LoadDimensionOperator(
    task_id = 'Load_song_dim_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    dim_table = 'songs'
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id = 'Load_artist_dim_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    dim_table = 'artists'
)

load_time_dimension_table = LoadDimensionOperator(
    task_id = 'Load_time_dim_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    dim_table = 'time'
)


# Data quality checks
songplays_quality_checks = DataQualityOperator(
    task_id = 'Songplays_quality_checks',
    dag = dag,
    redshift_conn_id = 'redshift',
    table = 'songplays',
    desired_checks = {   
            'has_rows': {},
            'row_count_between': {'lower_bound': 5000, 'upper_bound': 10000},
            'no_nulls': {'column': 'playid'},
            'all_distinct': {'column': 'playid'}
            }
)

users_quality_checks = DataQualityOperator(
    task_id = 'Users_quality_checks',
    dag = dag,
    redshift_conn_id = 'redshift',
    table = 'users',
    desired_checks = {   
            'has_rows': {},
            'row_count_between': {'lower_bound': 100, 'upper_bound': 500},
            'no_nulls': {'column': 'userid'},
            'all_distinct': {'column': 'userid'}
            }
)

songs_quality_checks = DataQualityOperator(
    task_id = 'Songs_quality_checks',
    dag = dag,
    redshift_conn_id = 'redshift',
    table = 'songs',
    desired_checks = {   
            'has_rows': {},
            'row_count_between': {'lower_bound': 10, 'upper_bound': 50},
            'no_nulls': {'column': 'songid'},
            'all_distinct': {'column': 'songid'}
            }
)

artists_quality_checks = DataQualityOperator(
    task_id = 'Artists_quality_checks',
    dag = dag,
    redshift_conn_id = 'redshift',
    table = 'artists',
    desired_checks = {   
            'has_rows': {},
            'row_count_between': {'lower_bound': 10, 'upper_bound': 50},
            'no_nulls': {'column': 'artistid'},
            'all_distinct': {'column': 'artistid'}
            }
)

time_quality_checks = DataQualityOperator(
    task_id = 'Time_quality_checks',
    dag = dag,
    redshift_conn_id = 'redshift',
    table = 'time',
    desired_checks = {   
            'has_rows': {},
            'row_count_between': {'lower_bound': 5000, 'upper_bound': 10000},
            'no_nulls': {'column': 'start_time'}
            }
)

end_operator = DummyOperator(task_id = 'Stop_execution',  dag = dag)


# Dependencies
start_operator >> create_tables_task

create_tables_task >> stage_events_to_redshift
create_tables_task >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_songplays_table >> songplays_quality_checks
load_user_dimension_table >> users_quality_checks
load_song_dimension_table >> songs_quality_checks
load_artist_dimension_table >> artists_quality_checks
load_time_dimension_table >> time_quality_checks

songplays_quality_checks >> end_operator
users_quality_checks >> end_operator
songs_quality_checks >> end_operator
artists_quality_checks >> end_operator
time_quality_checks >> end_operator