import os
import sys
from pathlib import Path

import pendulum
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator  # type: ignore
from pendulum import duration

sys.path.insert(0, os.path.dirname(Path(__file__).absolute().parent) + "/plugins")


from helpers import SqlQueries
from operators import (
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
    )

    stage_songs_to_redshift_task = StageToRedshiftOperator(
        task_id="Stage_songs",
    )

    load_songplays_task = LoadFactOperator(
        task_id="Load_songplays_fact_table",
    )

    load_user_task = LoadDimensionOperator(
        task_id="Load_user_dim_table",
    )

    load_songs_task = LoadDimensionOperator(
        task_id="Load_song_dim_table",
    )

    load_artist_task = LoadDimensionOperator(
        task_id="Load_artist_dim_table",
    )

    load_time_task = LoadDimensionOperator(
        task_id="Load_time_dim_table",
    )

    data_quality_task = DataQualityOperator(
        task_id="Run_data_quality_checks",
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
    )

    data_quality_task >> end_task


final_project_dag = sparkify_project()
