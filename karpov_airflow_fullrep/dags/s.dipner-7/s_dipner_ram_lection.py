# Airflow
from airflow import DAG
# —— Operators ——
from s_dipner_plugins.s_dipner_ram_practice import SdipnerRamSpeciesCountOperator

from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 's.dipner-7',
}

dag = DAG("sdipner_ram_lecture",
        schedule_interval="@daily",
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['karpov', 's.dipner-7']
        )

print_species_count = SdipnerRamSpeciesCountOperator(
    task_id="human_count",
    species_type="Human",
)

print_species_count