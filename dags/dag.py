from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from plugins.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from plugins.helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

# AWS S3 Bucket paths
LOG_DATA = 's3://udacity-dend/log_data'
LOG_JSON_PATH = 's3://udacity-dend/log_json_path.json'
SONG_DATA = 's3://udacity-dend/song_data'

# Default settings for my dag
default_args = {
    'owner': 'hyyyy',
    'start_date': datetime.now() - timedelta(minutes=10),
    'depends_on_past': False,
    'catchup': False,
    'email': ['lhongyu0509@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1
}

# Create dag which refresh every 5 minutes
dag = DAG(
    dag_id='dag_etl_redshift',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='*/5 * * * *'
)

# Dummy operator for beginning
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Create tables
create_tables = PostgresOperator(
    task_id='Create_tables',
    dag=dag,
    postgres_conn_id='redshift',
    sql='create_tables.sql'
)

# Tasks for creating and loading data into staging_events from S3 to Redshift cluster
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket=LOG_DATA,
    table='staging_events',
    json_format=LOG_JSON_PATH
)

# Tasks for creating and loading data into staging_songs from S3 to Redshift cluster
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket=SONG_DATA,
    table='staging_songs',
    json_format='auto'
)

# Loading fact table songplays from staging tables
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songplays',
    sql=SqlQueries.songplay_table_insert
)

# Loading dimensions table users from staging tables
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='users',
    sql=SqlQueries.user_table_insert,
    is_truncate=True
)

# Loading dimensions table songs from staging tables
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songs',
    sql=SqlQueries.song_table_insert,
    is_truncate=True
)

# Loading dimensions table artists from staging tables
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='artists',
    sql=SqlQueries.artist_table_insert,
    is_truncate=True
)

# Loading dimensions table times from staging tables
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='times',
    sql=SqlQueries.time_table_insert,
    is_truncate=True
)

# Check data quality we have processed above
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    table_list=['songplays', 'songs', 'artists', 'times', 'users'],
    expected_result=1,
)

# Dummy operator for ending
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Set dependencies for loading and transforming data with redshift
# For dag visualization please look at airflow UI
start_operator >> create_tables
create_tables >> stage_events_to_redshift >> load_songplays_table
create_tables >> stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
