import datetime
import pendulum
import os

import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


@dag(
    dag_id="kaggle_taste",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2023, 3, 30, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def LoadTaste():
    create_taste = PostgresOperator(
        task_id="create_taste",
        postgres_conn_id="data-test",
        sql="""
            CREATE TABLE IF NOT EXISTS taste (
                "Song_ID" VARCHAR PRIMARY KEY,
                "Track_ID" VARCHAR
            );""",
    )

    create_taste_temp = PostgresOperator(
        task_id="create_taste_temp",
        postgres_conn_id="data-test",
        sql="""
            DROP TABLE IF EXISTS taste_temp;
            CREATE TABLE taste_temp (
                "Song_ID" VARCHAR PRIMARY KEY,
                "Track_ID" VARCHAR
            );""",
    )

    @task
    def get_data():
        # NOTE: configure this as appropriate for your airflow environment
        data_path = "/opt/airflow/data/taste_profile_song_to_tracks.csv"
        os.makedirs(os.path.dirname(data_path), exist_ok=True)

        url = "https://raw.githubusercontent.com/JamisonUK/GroupA/develop/DataSet/taste_profile_song_to_tracks.csv"

        response = requests.request("GET", url)

        with open(data_path, "w") as file:
            file.write(response.text)

        postgres_hook = PostgresHook(postgres_conn_id="data-test")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        with open(data_path, "r") as file:
            cur.copy_expert(
                "COPY taste_temp FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                file,
            )
        conn.commit()

    @task
    def merge_data():
        query = """
            INSERT INTO taste
            SELECT *
            FROM (
                SELECT DISTINCT *
                FROM taste_temp
            ) t
            ON CONFLICT ("Song_ID") DO UPDATE
            SET "Song_ID" = excluded."Song_ID";
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

    [create_taste, create_taste_temp] >> get_data() >> merge_data()



dag = LoadTaste()
