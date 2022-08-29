import tempfile
from airflow.decorators import dag, task
from airflow.operators.python import PythonVirtualenvOperator
from airflow.models import Variable
from airflow import DAG
from datetime import datetime
from typing import Dict, List, Tuple
import logging


default_args = {
    'owner': 'dummy-model-maintainer',
    'depends_on_past': False,
}


def _get_metadata():
    import io, tempfile, zipfile, requests, redis
    from datetime import timedelta
    from pathlib import Path

    ORGANIZATION = 'MATRYCS'
    PROJECT_NAME = 'dummy-regression-model'
    BRANCH = 'main'

    GITHUB_REPO = f'https://github.com/{ORGANIZATION}/{PROJECT_NAME}/archive/{BRANCH}.zip'

    r = redis.Redis(host='redis', port=6379, db=0)

    # TODO: Virtualenv get destroyed in any case. Do it without tempdir
    with tempfile.TemporaryDirectory() as tmpdirname:
        res = requests.get(GITHUB_REPO, stream=True)
        buffer = io.BytesIO(res.content)

        r.set('dummy-model-object', buffer.read())

        z = zipfile.ZipFile(buffer)
        z.extractall(tmpdirname)

        print([str(x) for x in Path(tmpdirname).iterdir()])

        with open(f'{tmpdirname}/{PROJECT_NAME}-{BRANCH}/requirements.txt') as fp:
            requirements = fp.read().split()

        return {'requirements': requirements, 'project_name': PROJECT_NAME, 'branch': BRANCH}

def _build_model(project_name, branch):
    import os, sys, io, redis, zipfile, tempfile, subprocess
    from pathlib import Path

    # MLFlow script accepts will look for environment variable where server is located
    #os.environ['MLFLOW_TRACKING_URI'] = 'http://172.17.0.1:5000/'

    r = redis.Redis(host='redis', port=6379, db=0)

    with tempfile.TemporaryDirectory() as tmpdirname:
        buffer = io.BytesIO(r.get('dummy-model-object'))
        z = zipfile.ZipFile(buffer)
        z.extractall(tmpdirname)

        result = subprocess.run(
            args=[sys.executable, 'train.py'],
            capture_output=True,
            text=True,
            cwd=f'{tmpdirname}/{project_name}-{branch}',
            # Override environment variable(s)
            env={'MLFLOW_TRACKING_URI': 'http://172.17.0.1:5000/'},
        )

        print("stdout:", result.stdout)
        print("stderr:", result.stderr)


with DAG(
    'retrain-dummy-model',
    default_args=default_args,
    schedule_interval='@once',
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['test', 'dummy-model', 'mlflow'],
) as dag:

    get_metadata = PythonVirtualenvOperator(
        task_id='get_metadata',
        python_callable=_get_metadata,
        requirements=['requests', 'redis'],
        do_xcom_push=True,
    )

    build_model = PythonVirtualenvOperator(
        task_id='build_model',
        python_callable=_build_model,
        op_kwargs={
            'project_name': '{{ ti.xcom_pull(task_ids="get_metadata").project_name }}',
            'branch': '{{ ti.xcom_pull(task_ids="get_metadata").branch }}'
        },
        requirements='''
            redis
            {{ ti.xcom_pull(task_ids="get_metadata").requirements | join("\n") }}
        ''',
        do_xcom_push=True,
    )

    get_metadata >> build_model
