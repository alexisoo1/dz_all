import config
from dz1 import dz1
from dz2 import dz2
from dz3 import dz3
from pathlib import Path
import logging 
from datetime import datetime, timedelta
 
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.decorators import dag, task
conId = 'sqlite_default'

default_args = {
    'owner': 'sayfulin',
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

logger = logging.getLogger(__name__)
logger.setLevel(config.loggerLevel)
lfm = logging.Formatter(config.formatter)
lsh = logging.StreamHandler()
lsh.setLevel(config.streamLogHandlerLevel)
lsh.setFormatter(lfm)
lfh = logging.FileHandler(filename='log.log', mode='w')
lfh.setFormatter(lfm)
lfh.setLevel(config.fileLogHandlerLevel)
logger.addHandler(lsh)
logger.addHandler(lfh)

print(f"Начато {datetime.now()}")
logger.info(f"Начато {datetime.now()}")

okvedZip = Path(config.dataDir, config.okvedZip)
okvedPath = Path(config.dataDir, Path(config.okvedZip).stem)
okved = Path(config.dataDir, Path(config.okvedZip).stem, config.okved)
egrulZip = Path(config.dataDir, config.egrulZip)
egrulPath = Path(config.dataDir, Path(config.egrulZip).stem)

sayfulinDZ3 = DAG(
    dag_id='sayfulin_dz',
    default_args=default_args,
    description='DAG for process okved, egrul and vacancies',
    start_date=datetime(2023, 8, 7, 12),
    schedule_interval='@daily'
) 

downloadOkved = BashOperator(
    task_id='download_okved_file',
    #bash_command=f"wget {config.downloadOkvedPath} -O {okvedZip}",
    bash_command = "echo download_okved_file",
    dag = sayfulinDZ3
)

downloadEgrul = BashOperator(
    task_id='download_egrul_file',
    #bash_command=f"wget {config.downloadEgrulPath} -O {egrulZip}",
    bash_command = "echo download_egrul_file",
    dag = sayfulinDZ3
)

dz1 = PythonOperator(
    task_id='dz1',
    python_callable=dz1,
    dag = sayfulinDZ3
)

dz2 = PythonOperator(
    task_id='dz2',
    python_callable=dz2,
    dag = sayfulinDZ3
)

dz3 = PythonOperator(
    task_id='dz3',
    python_callable=dz3,
    dag = sayfulinDZ3
)

downloadOkved.set_downstream(dz1)
downloadEgrul.set_downstream(dz1)
dz1.set_downstream(dz3)
dz2.set_downstream(dz3)