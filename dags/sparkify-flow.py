import os
import sys
from pathlib import Path

import pendulum
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator  # type: ignore
from pendulum import duration

sys.path.insert(0, os.path.dirname(Path(__file__).absolute().parent) + "/plugins")

from helpers.sql_queries import SqlQueries  # type: ignore
from operators import (  # type: ignore
    DataQualityOperator,
    LoadDimensionOperator,
    LoadFactOperator,
    StageToRedshiftOperator,
)

default_args = {
    "owner": "rosapham",
    "start_date": pendulum.now(),
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": duration(minutes=5),
    "catchup": False,
    "email_on_retry": False,
    "schedule_interval": "@hourly",
}


@dag(default_args=default_args, description="Load and transform data in Redshift with Airflow")
def sparkify_project() -> None:

    begin_task = DummyOperator(task_id="Begin_execution")

    stage_events_to_redshift_task = StageToRedshiftOperator(
        task_id="Stage_events",
        table="staging_events",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket=Variable.get("s3_bucket"),
        s3_key="log-data",
        json_path="log_json_path.json",
        create_sql=SqlQueries.staging_events_table_create,
    )

    stage_songs_to_redshift_task = StageToRedshiftOperator(
        task_id="Stage_songs",
        table="staging_songs",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket=Variable.get("s3_bucket"),
        s3_key="song-data",
        create_sql=SqlQueries.staging_songs_table_create,
    )

    load_songplays_task = LoadFactOperator(
        task_id="Load_songplays_fact_table",
        table="songplays",
        redshift_conn_id="redshift",
        is_appending=True,
        create_sql=SqlQueries.songplays_table_create,
        insert_sql=SqlQueries.songplays_table_insert,
    )

    load_user_task = LoadDimensionOperator(
        task_id="Load_user_dim_table",
        table="users",
        redshift_conn_id="redshift",
        create_sql=SqlQueries.users_table_create,
        insert_sql=SqlQueries.users_table_insert,
        is_truncating=True,
    )

    load_songs_task = LoadDimensionOperator(
        task_id="Load_song_dim_table",
        table="songs",
        redshift_conn_id="redshift",
        create_sql=SqlQueries.songs_table_create,
        insert_sql=SqlQueries.songs_table_insert,
        is_truncating=True,
    )

    load_artist_task = LoadDimensionOperator(
        task_id="Load_artist_dim_table",
        table="artists",
        redshift_conn_id="redshift",
        create_sql=SqlQueries.artists_table_create,
        insert_sql=SqlQueries.artists_table_insert,
        is_truncating=True,
    )

    load_time_task = LoadDimensionOperator(
        task_id="Load_time_dim_table",
        table="time",
        redshift_conn_id="redshift",
        create_sql=SqlQueries.time_table_create,
        insert_sql=SqlQueries.time_table_insert,
        is_truncating=True,
    )

    data_quality_task = DataQualityOperator(
        task_id="Run_data_quality_checks",
        redshift_conn_id="redshift",
        tables=["songplays", "users", "songs", "artists", "time"],
        entities={"songplays": "userid"},
    )

    end_task = DummyOperator(task_id="End_execution")

    (
        begin_task
        >> [stage_events_to_redshift_task, stage_songs_to_redshift_task]
        >> load_songplays_task
    )

    (
        load_songplays_task
        >> [
            load_user_task,
            load_songs_task,
            load_artist_task,
            load_time_task,
        ]
        >> data_quality_task
        >> end_task
    )


final_project_dag = sparkify_project()
