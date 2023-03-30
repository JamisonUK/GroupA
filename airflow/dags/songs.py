import datetime
import pendulum
import os

import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


@dag(
    dag_id="kaggle_songs",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2023, 3, 30, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def LoadSongs():
    create_songs = PostgresOperator(
        task_id="create_songs",
        postgres_conn_id="data-test",
        sql="""
            CREATE TABLE IF NOT EXISTS songs (
                "Track_ID" VARCHAR PRIMARY KEY,
                "ID" INTEGER
            );""",
    )

    create_songs_temp = PostgresOperator(
        task_id="create_songs_temp",
        postgres_conn_id="data-test",
        sql="""
            DROP TABLE IF EXISTS songs_temp;
            CREATE TABLE songs_temp (
                "Track_ID" VARCHAR PRIMARY KEY,
                "ID" INTEGER
            );""",
    )

    @task
    def get_data():
        # NOTE: configure this as appropriate for your airflow environment
        data_path = "/opt/airflow/data/kaggle_songs.csv"
        os.makedirs(os.path.dirname(data_path), exist_ok=True)

        url = "https://raw.githubusercontent.com/JamisonUK/GroupA/develop/DataSet/kaggle_songs.csv"

        response = requests.request("GET", url)

        with open(data_path, "w") as file:
            file.write(response.text)

        postgres_hook = PostgresHook(postgres_conn_id="data-test")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        with open(data_path, "r") as file:
            cur.copy_expert(
                "COPY songs_temp FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                file,
            )
        conn.commit()

    @task
    def merge_data():
        query = """
            INSERT INTO songs
            SELECT *
            FROM (
                SELECT DISTINCT *
                FROM songs_temp
            ) t
            ON CONFLICT ("Track_ID") DO UPDATE
            SET "Track_Id" = excluded."Track_ID";
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

    [create_songs, create_songs_temp] >> get_data() >> merge_data()



dag = LoadSongs()
