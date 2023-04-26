import datetime
import pendulum
import os

import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


@dag(
    dag_id="kaggle_users",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2023, 3, 30, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def LoadUsers():
    create_users = PostgresOperator(
        task_id="create_users",
        postgres_conn_id="data-test",
        sql="""
            CREATE TABLE IF NOT EXISTS users (
                "User_ID" VARCHAR PRIMARY KEY,
            );""",
    )

    create_users_temp = PostgresOperator(
        task_id="create_users_temp",
        postgres_conn_id="data-test",
        sql="""
            DROP TABLE IF EXISTS users_temp;
            CREATE TABLE users_temp (
                "User_ID" VARCHAR PRIMARY KEY,
            );""",
    )

    @task
    def get_data():
        # NOTE: configure this as appropriate for your airflow environment
        data_path = "/opt/airflow/data/kaggle_users.csv"
        os.makedirs(os.path.dirname(data_path), exist_ok=True)

        url = "https://raw.githubusercontent.com/JamisonUK/GroupA/develop/DataSet/kaggle_users.csv"

        response = requests.request("GET", url)

        with open(data_path, "w") as file:
            file.write(response.text)

        postgres_hook = PostgresHook(postgres_conn_id="data-test")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        with open(data_path, "r") as file:
            cur.copy_expert(
                "COPY users_temp FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                file,
            )
        conn.commit()

    @task
    def merge_data():
        query = """
            INSERT INTO users
            SELECT *
            FROM (
                SELECT DISTINCT *
                FROM users_temp
            ) t
            ON CONFLICT ("User_ID") DO UPDATE
            SET "User_ID" = excluded."User_ID";
        """
        try:
            postgres_hook = PostgresHook(postgres_conn_id="data-test")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()
            return 0
        except Exception as e:
            return 1

    [create_users, create_users_temp] >> get_data() >> merge_data()



dag = LoadUsers()