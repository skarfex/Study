import logging

from airflow import DAG
from datetime import datetime

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'd-matveev',
    'poke_interval': 600
}

with DAG("art_dag_d-matveev",
    schedule_interval='0 0 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['art_dag_d-matveev']
) as dag:

    
    def greenplum_query(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        week_day = datetime.strptime(kwargs['ds'],'%Y-%m-%d').weekday()+1 # берем дату из Jinja
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        cursor.execute(f'SELECT heading FROM articles WHERE id = { week_day }')  # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        return  query_res 

    get_greeenplum_data_operator = PythonOperator(
        task_id='get_greeenplum_data_operator',
        python_callable=greenplum_query,
        provide_context=True
    )

    
    def print_result(** kwargs):
        logging.info(kwargs['templates_dict']['tbl_row_text'])
        
    
    print_result_operator=PythonOperator(task_id="print_result_operator",
                                         python_callable=print_result,
                                         templates_dict={'tbl_row_text': '{{ ti.xcom_pull(task_ids="get_greeenplum_data_operator", key="return_value") }}'},
                                         provide_context=True
                                         )
    
    
    get_greeenplum_data_operator >> print_result_operator
    