"""
DAG загрузки top 3 локации сериала "Рик и Морти" с наибольшим количеством резидентов.
Целевая таблица: public."v-ivanchenko-11_ram_location"
"""
from datetime import datetime

from airflow import DAG

from v_ivanchenko_11_plugins.v_ivanchenko_11_operators import (
    IvanchenkoLoadRamLocationToStage,
    IvanchenkoLoadRamLocation,
)

DEFAULT_ARGS = {
    'start_date': datetime(2022, 1, 1),
    'owner': 'v-ivanchenko-11',
}

with DAG("v-ivanchenko-11-ram-location-load",
         schedule_interval=None,
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['v-ivanchenko-11']
         ) as dag:

    load_ram_location_to_stage = IvanchenkoLoadRamLocationToStage(
        task_id='load_ram_location_stage'
    )

    load_ram_location = IvanchenkoLoadRamLocation(
        task_id='load_ram_location'
    )

    load_ram_location_to_stage >> load_ram_location






