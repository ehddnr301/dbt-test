import os
import json
from abc import ABC, abstractmethod
from datetime import datetime, timedelta

import psycopg2
import pandas as pd
from sqlalchemy import create_engine, text

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


class ABCTransfer(ABC):
    def __init__(self, source_conn, target_conn, table_name):
        self.source_conn = source_conn
        self.target_conn = target_conn
        self.table_name = table_name

    @abstractmethod
    def transfer(self):
        pass


class SimplePostgresTransfer(ABCTransfer):
    def transfer(self):
        current_time = datetime.now().replace(minute=0, second=0, microsecond=0)

        last_hour_time = current_time - timedelta(hours=1, seconds=1)

        current_time_str = current_time.isoformat().replace("T", " ")
        last_hour_time_str = last_hour_time.isoformat().replace("T", " ")

        query = f"SELECT * FROM {self.table_name} WHERE created_at > '{last_hour_time_str}' AND created_at <= '{current_time_str}'"
        df = pd.read_sql(query, self.source_conn)

        columns_to_drop = ["recommended_movie_id_list", "genres"]
        columns_to_drop = [col for col in columns_to_drop if col in df.columns]
        if columns_to_drop:
            df = df.drop(columns=columns_to_drop)

        delete_query = f"DELETE FROM {self.table_name} WHERE created_at > '{last_hour_time_str}' AND created_at <= '{current_time_str}'"
        self.target_conn.execute(text(delete_query))

        df.to_sql(self.table_name, self.target_conn, if_exists="append", index=False)


def run_transfer(*args, **kwargs):
    source_engine = create_engine(
        "postgresql+psycopg2://postgres:postgres@postgres-service/postgres"
    )
    target_engine = create_engine(
        "postgresql+psycopg2://postgres:postgres@postgres-service-target/postgres"
    )
    table_name = kwargs["table_name"]
    transfer = SimplePostgresTransfer(source_engine, target_engine, table_name)
    transfer.transfer()

def send_email(*args, **kwargs):
    print("Sending email...")
    # 이메일 보내는 로직

default_args = {
    "owner": "dwlee",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


for filename in os.listdir("/opt/airflow/dags"):
    if filename.endswith(".json"):
        dag_id = filename[:-5]
        with open(f"/opt/airflow/dags/{filename}", "r") as f:
            config = json.load(f)

        table_name = config["table_name"]
        schedule_interval = config["schedule_interval"]
        catchup = config.get("catchup", "False") == "True"
        start_date_str = config["start_date"]
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")

        globals()[dag_id] = DAG(
            dag_id,
            default_args=default_args,
            description=f"Load data into {table_name}",
            schedule_interval=schedule_interval,
            start_date=start_date,
            catchup=catchup,
        )

        load_data_task = PythonOperator(
            task_id=f"load_data_into_{table_name}",
            python_callable=run_transfer,
            op_kwargs={"table_name": table_name},
            dag=globals()[dag_id],
        )

        send_email_task = PythonOperator(
            task_id='send_email',
            python_callable=send_email,
            dag=globals()[dag_id],
        )

        load_data_task >> send_email_task