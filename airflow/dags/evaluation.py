import datetime
import pendulum
import os

import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

# dags
@dag(
    dag_id="kaggle_evaluation",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2023, 3, 30, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def LoadEvaluation():
    create_evaluation = PostgresOperator(
        task_id="create_evaluation",
        postgres_conn_id="data-test",
        sql="""
            CREATE TABLE IF NOT EXISTS evaluation (
                "User_ID" VARCHAR PRIMARY KEY,
                "Song_ID" VARCHAR,
                "Plays" INTEGER
            );""",
    )

    create_evaluation_temp = PostgresOperator(
        task_id="create_evaluation_temp",
        postgres_conn_id="data-test",
        sql="""
            DROP TABLE IF EXISTS evaluation_temp;
            CREATE TABLE evaluation_temp (
                "User_ID" VARCHAR PRIMARY KEY,
                "Song_ID" VARCHAR,
                "Plays" INTEGER
            );""",
    )

    @task
    def get_data():
        # NOTE: configure this as appropriate for your airflow environment
        data_path = "/opt/airflow/data/kaggle_visible_evaluation_triplets.csv"
        os.makedirs(os.path.dirname(data_path), exist_ok=True)

        url = "https://raw.githubusercontent.com/JamisonUK/GroupA/develop/DataSet/kaggle_visible_evaluation_triplets.csv"

        response = requests.request("GET", url)

        with open(data_path, "w") as file:
            file.write(response.text)

        postgres_hook = PostgresHook(postgres_conn_id="data-test")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        with open(data_path, "r") as file:
            cur.copy_expert(
                "COPY evaluation_temp FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                file,
            )
        conn.commit()

    @task
    def merge_data():
        query = """
            INSERT INTO evaluation
            SELECT *
            FROM (
                SELECT DISTINCT *
                FROM evaluation_temp
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

    [create_evaluation, create_evaluation_temp] >> get_data() >> merge_data()



dag = LoadEvaluation()