import json 
import pathlib 
import datetime as dt

import pandas as pd
import airflow 
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


dag = DAG(dag_id="01_unscheduled",
                start_date=dt.datetime(2019,3,6),
                schedule_interval=None)
fetch_events = BashOperator(task_id="fetch_events",
                    bash_command="curl -o /tmp/events.json https://localhost:5000/events",
                    dag=dag)


def _calculate_stats(input_path, output_path):
    """Calculates event statistics."""
    events = pd.read_json(input_path)
    stats = events.groupby(["date", "user"]).size().reset_index()
    pathlib.Path(output_path).parent.mkdir(exist_ok=True)
    stats.to_csv(output_path, index=False)




calculate_stats = PythonOperator(
    task_id="calculate_stats",
    python_callable=_calculate_stats,
    dag=dag,
)

fetch_events >> calculate_stats 