import datetime
import pendulum
import os

import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


@dag(
    dag_id="kaggle_tracks",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2023, 3, 30, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def LoadTracks():
    create_tracks = PostgresOperator(
        task_id="create_tracks",
        postgres_conn_id="data-test",
        sql="""
            CREATE TABLE IF NOT EXISTS tracks (
                track_id            varchar PRIMARY KEY,
                title               varchar,
                song_id             varchar,
                release             varchar,
                artist_id           varchar,
                artist_mbid         varchar,
                artist_name         varchar,
                duration            real,
                artist_familiarity  real,
                artist_hotttnesss   real,
                year                int,
                track_7digitalid    int,
                shs_perf            int,
                shs_work            int
            );""",
    )

    create_tracks_temp = PostgresOperator(
        task_id="create_tracks_temp",
        postgres_conn_id="data-test",
        sql="""
            DROP TABLE IF EXISTS tracks_temp;
            CREATE TABLE tracks_temp (
                track_id            varchar PRIMARY KEY,
                title               varchar,
                song_id             varchar,
                release             varchar,
                artist_id           varchar,
                artist_mbid         varchar,
                artist_name         varchar,
                duration            real,
                artist_familiarity  real,
                artist_hotttnesss   real,
                year                int,
                track_7digitalid    int,
                shs_perf            int,
                shs_work            int
            );""",
    )

    @task
    def get_data():
        # NOTE: configure this as appropriate for your airflow environment
        data_path = "/opt/airflow/data/track_metadata.csv"
        os.makedirs(os.path.dirname(data_path), exist_ok=True)

        url = "https://raw.githubusercontent.com/JamisonUK/GroupA/develop/DataSet/track_metadata.csv"

        response = requests.request("GET", url)

        with open(data_path, "w") as file:
            file.write(response.text)

        postgres_hook = PostgresHook(postgres_conn_id="data-test")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        with open(data_path, "r") as file:
            cur.copy_expert(
                "COPY tracks_temp FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                file,
            )
        conn.commit()

    @task
    def merge_data():
        query = """
            INSERT INTO tracks
            SELECT *
            FROM (
                SELECT DISTINCT *
                FROM tracks_temp
            ) t
            ON CONFLICT ("Track_ID") DO UPDATE
            SET "Track_ID" = excluded."Track_ID";
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

    [create_tracks, create_tracks_temp] >> get_data() >> merge_data()



dag = LoadTracks()
