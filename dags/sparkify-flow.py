import logging

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator


def hello_world() -> None:
    logging.info("Hello World!")


dag = DAG("greet_flow_dag_legacy", start_date=pendulum.now())


greet_task = PythonOperator(task_id="hello_world_task", python_callable=hello_world, dag=dag)
