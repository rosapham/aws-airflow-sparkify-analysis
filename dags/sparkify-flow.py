import os
import sys
from pathlib import Path

import pendulum
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator  # type: ignore

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
}


@dag(default_args=default_args, description="Load and transform data in Redshift with Airflow")
def sparkify_project() -> None:

    DummyOperator(task_id="Begin_execution")

    StageToRedshiftOperator(
        task_id="Stage_events",
    )

    StageToRedshiftOperator(
        task_id="Stage_songs",
    )

    LoadFactOperator(
        task_id="Load_songplays_fact_table",
    )

    LoadDimensionOperator(
        task_id="Load_user_dim_table",
    )

    LoadDimensionOperator(
        task_id="Load_song_dim_table",
    )

    LoadDimensionOperator(
        task_id="Load_artist_dim_table",
    )

    LoadDimensionOperator(
        task_id="Load_time_dim_table",
    )

    DataQualityOperator(
        task_id="Run_data_quality_checks",
    )


final_project_dag = sparkify_project()
