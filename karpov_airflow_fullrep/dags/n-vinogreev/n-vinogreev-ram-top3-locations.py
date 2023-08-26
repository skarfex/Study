from airflow import DAG
from airflow.utils.dates import days_ago
from n_vinogreev.n_vinogreev_ram_top3_residents_count import NVinogreevRAMResidentsTopLocationsOperator

with DAG(
    dag_id='n-vinogreev-ram-top3-locations',
    schedule_interval='@once',
    tags=['n_vinogreev'],
    start_date=days_ago(0)
) as dag:

    #def print_http_hook():
    #    http_hook = HttpHook(http_conn_id='dina_ram', method='GET')
    #    logging.info(str(http_hook.run(endpoint='api/character').json()['info']['pages']))


    print_http_hook = NVinogreevRAMResidentsTopLocationsOperator(
        task_id='print_http_hook',
        number_of_top=3,
        http_conn_id='dina_ram',
        postgres_conn_id='conn_greenplum_write',
        dag=dag
    )

    print_http_hook
