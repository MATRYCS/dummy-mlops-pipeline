import tempfile
from airflow.decorators import dag, task
from airflow.operators.python import PythonVirtualenvOperator, PythonOperator
from airflow import DAG
from datetime import datetime
from typing import Dict, List, Tuple
import logging


MLFLOW_TRACKING_URI = 'http://172.17.0.1:5000/'
REGISTERED_MODEL_NAME = 'matrycs-dummy-regressor'
MODEL_STAGE = 'Production'

MODEL_URI = f'models:/{REGISTERED_MODEL_NAME}/{MODEL_STAGE}'


default_args = {
    'owner': 'dummy-model-maintainer',
    'depends_on_past': False,
}


def _fetch_model_dependecies(mlflow_tracking_uri:str, model_uri:str):
    import mlflow
    import mlflow.artifacts
    import tempfile

    # Configure MLFLow settings to be aware of remote tracker server
    mlflow.set_tracking_uri(mlflow_tracking_uri)

    with tempfile.TemporaryDirectory() as tmpdirname:
        mlflow.artifacts.download_artifacts(model_uri, dst_path=tmpdirname)
        with open(f'{tmpdirname}/requirements.txt', mode='r') as fp:
            requirements = fp.read().split()
            requirements = tuple(filter(None, requirements))

            requirements = tuple(filter(lambda x: 'apache' not in x, requirements))            

    return {
        'requirements': requirements
    }


def _build_bento(mlflow_tracking_uri:str, model_uri:str):
    import mlflow
    import bentoml

    # Configure MLFLow settings to be aware of remote tracker server
    mlflow.set_tracking_uri(mlflow_tracking_uri)

    bentoml_model = bentoml.mlflow.import_model(
        'dummy-model-regressor',
        model_uri=model_uri,
        signatures={'predict': {'batchable': True}}
    )


with DAG(
    'deploy-dummy-model',
    default_args=default_args,
    schedule_interval='@once',
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['test', 'dummy-model', 'mlflow'],
) as dag:

    fetch_model_dependecies = PythonVirtualenvOperator(
        task_id='fetch-model-dependencies',
        python_callable=_fetch_model_dependecies,
        requirements=['mlflow-skinny==1.26.1'],
        op_kwargs={
            'mlflow_tracking_uri': MLFLOW_TRACKING_URI,
            'model_uri': MODEL_URI,
        },
        do_xcom_push=True,
    )

    build_bento = PythonVirtualenvOperator(
        task_id='build-bento',
        python_callable=_build_bento,
        requirements='''
            mlflow-skinny==1.26.1
            bentoml>=1.0
            {{ ti.xcom_pull(task_ids="fetch-model-dependencies").requirements | join("\n") }}
        ''',
        op_kwargs={
            'mlflow_tracking_uri': MLFLOW_TRACKING_URI,
            'model_uri': MODEL_URI,
        },
        do_xcom_push=True,
    )

    fetch_model_dependecies >> build_bento
