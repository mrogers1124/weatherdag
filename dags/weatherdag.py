import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable


def extract_from_nws_api(**context):
    from weatherdag_utils import get_from_nws_api, parse_nws_api_response, save_observations
    config = Variable.get("weatherdag_config", deserialize_json=True)
    response = get_from_nws_api(
        stationId=config["stationId"],
        start=context["data_interval_start"],
        end=context["data_interval_end"],
    )
    observations = parse_nws_api_response(response)
    save_observations(
        observations=observations,
        out_dir=os.path.join(config["raw_folder"], context["ds_nodash"])
    )


with DAG(
    "weatherdag",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5)
    },
    description="A DAG for extracting National Weather Service observation data.",
    schedule="@daily",
    start_date=datetime(2023, 6, 1),
    catchup=True,
) as dag:

    task_extract_from_nws_api = PythonOperator(
        task_id="extract_from_nws_api",
        python_callable=extract_from_nws_api,
        provide_context=True,
    )

    task_start = EmptyOperator(
        task_id='start'
    )

    task_finish = EmptyOperator(
        task_id='finish'
    )

    task_start >> task_extract_from_nws_api >> task_finish
