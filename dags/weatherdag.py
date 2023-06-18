import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# Load Airflow variables
weatherdag_config = Variable.get("weatherdag_config", deserialize_json=True)
weatherdag_stationId = weatherdag_config["stationId"]
weatherdag_raw_folder = weatherdag_config["raw_folder"]
weatherdag_db_path = weatherdag_config["db_path"]
weatherdag_viz_path = weatherdag_config["viz_path"]


def extract_from_nws_api(**context):
    from weatherdag_utils import get_from_nws_api, parse_nws_api_response, save_observations
    response = get_from_nws_api(
        stationId=weatherdag_stationId,
        start=context["data_interval_start"],
        end=context["data_interval_end"],
    )
    observations = parse_nws_api_response(response)
    save_observations(
        observations=observations,
        out_dir=os.path.join(weatherdag_raw_folder, context["ds_nodash"])
    )


def load_raw_layer(**context):
    from weatherdag_utils import load_observations
    load_observations(
        in_dir=os.path.join(weatherdag_raw_folder, context["ds_nodash"]),
        db_path=weatherdag_db_path,
        run_id=context["run_id"],
        sys_load_time=datetime.utcnow().isoformat()
    )


def update_visualization():
    from weatherdag_utils import update_viz
    update_viz(db_path=weatherdag_db_path, viz_path=weatherdag_viz_path)


def update_db():
    from weatherdag_utils import create_or_update_db
    create_or_update_db(weatherdag_db_path)


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

    task_start = EmptyOperator(task_id='start')
    task_finish = EmptyOperator(task_id='finish')

    task_extract_from_nws_api = PythonOperator(
        task_id="extract_from_nws_api",
        python_callable=extract_from_nws_api,
        provide_context=True,
    )

    task_load_raw_layer = PythonOperator(
        task_id="load_raw_layer",
        python_callable=load_raw_layer,
        provide_context=True,
    )

    task_update_viz = PythonOperator(
        task_id="task_update_viz",
        python_callable=update_visualization,
    )

    task_start >> task_extract_from_nws_api >> task_load_raw_layer >> task_update_viz >> task_finish


with DAG(
    "weatherdag_update_db",
    default_args={
        "depends_on_past": False,
        "retries": 0,
    },
    description="A DAG for updating the weatherdag database.",
    start_date=datetime(2023, 1, 1),
    schedule=None,
) as dag:

    task_start = EmptyOperator(task_id='start')
    task_finish = EmptyOperator(task_id='finish')
    task_update_db = PythonOperator(
        task_id="update_db",
        python_callable=update_db
    )

    task_start >> task_update_db >> task_finish
