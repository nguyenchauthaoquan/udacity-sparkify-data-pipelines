import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

from common.operators.create_tables import CreateTablesOperator
from common.operators.load_fact import LoadFactOperator
from common.operators.load_dimension import LoadDimensionOperator
from common.operators.data_quality import DataQualityOperator
from common.operators.stage_redshift import StageToRedshiftOperator
from common.helpers.sql_queries import SqlQueries

default_args = {
    'owner': 'Haytham Nguyen',
    'depends_on_past': False,
    'start_date': pendulum.now(),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG('sparkify-dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
          )

start_operator = CreateTablesOperator(
    task_id='Begin_execution',
    dag=dag,
    connection_id="aws_redshift_connection",
    sql_file="/opt/airflow/scripts/create_tables.sql"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    region="us-east-1",
    s3={
        "bucket_name": "quannct-sparkify",
        "prefix": "log-data"
    },
    connection_id={
        "credentials": "aws_iam_credentials",
        "redshift": "aws_redshift_connection"
    }
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    region="us-east-1",
    s3={
        "bucket_name": "quannct-sparkify",
        "prefix": "song-data"
    },
    connection_id={
        "credentials": "aws_iam_credentials",
        "redshift": "aws_redshift_connection"
    }
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    connection_id="aws_redshift_connection",
    table="songplays",
    sql_query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    connection_id="aws_redshift_connection",
    table="users",
    sql_query=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    connection_id="aws_redshift_connection",
    table="songs",
    sql_query=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    connection_id="aws_redshift_connection",
    table="artists",
    sql_query=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    connection_id="aws_redshift_connection",
    table="artists",
    sql_query=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    connection_id={
        "credentials": "aws_iam_credentials",
        "redshift": "aws_redshift_connection"
    },
    tables=["songplay", "users", "song", "artist", "time"]
)

end_operator = EmptyOperator(task_id='Stop_execution', dag=dag)

start_operator >> [stage_songs_to_redshift, stage_events_to_redshift]

[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table,
                         load_time_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator
