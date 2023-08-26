"""
Конвейер для решения учебной задачи:
- загрузить курсы валют;
- загрузить перечень товаров с их стоимостью;
- выбрать валюту с наибольшим курсом;
- произвести перерасчёт стоимости товаров с зависимости курса от
  выбранной валюты;
- сохранить полученный результат в файл.

Для академичности алгоритм реализован c использованием разветвления (branch),
что, конечно, изыточно в рамках данной решаемой задачи.

Вопросы:
- Куда по-умолчанию происходит запись в файловой системе
  (см. результаты задач store, list)?

Внимание! Версии программного обеспечения на сервере
          (python: 3.7.12; airflow: 2.2.4)
Вместо привычных конструкции необходимо использовать следующие:
- f'{value=}' ==> f'{value}'
- dict[str, float] ==> dict либо Dict[str, float] (+import typing)

Подсказка:
- @task(multiple_outputs=True) используется только в случае если функция
  возвращает тип dict(). При несоответствии выбрасывается исключение, например:
  Returned output was type <class 'float'> expected dictionary for multiple_outputs;

  By using the typing Dict for the function return type, the multiple_outputs
  parameter is automatically set to True.
- Результат выполнения задачи (@task) автоматически записывается в журнал (log);
- Экспорт функции из внешнего файла: from include.my_file import my_function
"""


import logging
import pendulum as pdl

from airflow import DAG
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

from airflow.operators.bash import BashOperator as cmd
from airflow.operators.dummy import DummyOperator as empty
from airflow.operators.python import BranchPythonOperator as branch
from airflow.utils.trigger_rule import TriggerRule



#
# DAG
#
@dag(
    dag_id='l-fedjaev-13-pipeline',

    schedule_interval='@hourly',
    start_date=pdl.today('UTC').add(days=-2),
    max_active_runs=1,
    catchup=False,

    tags=['leo', 'ranks', 'goods', 'pipeline'],

    # наследуются всеми задачами
    default_args=dict(
        owner='l-fedjaev-13', # airflow login
        poke_interval=100,
    ),
)
def pipeline():
    @task(multiple_outputs=True)
    def load_ranks(api: str) -> bool:
        """
        Загрузить актуальные курсы валют с сайта псевдоцентрального банка РФ
        """

        import random
        random.seed(100)

        logging.info(f'Currency ranks loaded from {api}')

        return dict(usd=random.uniform(15, 41), eur=random.uniform(10, 35))


    @task(multiple_outputs=True)
    def load_goods(api: str) -> bool:
        """
        Загрузить перечень товаров и их стоимость в условных единицах
        с Московской псевдотоварно-сырьвой биржи.
        """

        logging.info(f'Goods loaded from {api}')

        return {'картофель': 2.3, 'рожь': 1.7, 'лён': 4.6,}


    #@task.branch <== еще не завезли :-(
    def decision(**kwargs) -> str:
        """
        Выбрать валюту, курс которой выше.
        """

        ranks = kwargs['ti'].xcom_pull(task_ids='load_ranks')

        for cur in ['usd', 'eur']:
            if not cur in ranks.keys():
                raise AirflowFailException(f'Currency "{cur}" not found in ranks.')

        return 'transform_by_{}'.format('usd' if ranks['usd'] > ranks['eur'] else 'eur')


    @task(multiple_outputs=True)
    def transform_by_usd(**kwargs) -> dict:
        """Рассчитать стоимость товаров исходя из курса"""

        # получить данные (XCom)
        ranks = kwargs['ti'].xcom_pull(task_ids='load_ranks')
        goods = kwargs['ti'].xcom_pull(task_ids='load_goods')

        return {k: v * ranks['usd'] for k, v in goods.items()}


    @task(multiple_outputs=True)
    def transform_by_eur(**kwargs) -> dict:
        """Рассчитать стоимость товаров исходя из курса"""

        # получить данные (XCom)
        ranks = kwargs['ti'].xcom_pull(task_ids='load_ranks')
        goods = kwargs['ti'].xcom_pull(task_ids='load_goods')

        return {k: v * ranks['eur'] for k, v in goods.items()}


    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def store(**kwargs) -> bool:
        """
        Сохранить полученный результат.

        Т.к. не известно какой курс будет выше, то также, соответственно,
        не известно какая задача пересчёта будет выполнена, поэтому
        проверяем оба из возможных результатов на существование и работаем
        с тем, который существует.

        По-умолчанию, задача будет выполнена, если её непосредственные
        предшественники (в данном случае transform_by_usd, tarnsform_by_eur)
        будут успешно выполнены (статус: ALL_SUCCESS), т.к. это невозможно
        в связи с тем, что из них всегда выполнятеся только одна, то
        необходимо установить соответствующий флаг в переменной =trigger_rule=.
        """

        import numpy as np

        goods_usd = kwargs['ti'].xcom_pull(task_ids='transform_by_usd')
        goods_eur = kwargs['ti'].xcom_pull(task_ids='transform_by_eur')

        result = goods_usd if not goods_usd is None else goods_eur

        logging.info(f'goods: {result}')

        np.save('result.npy', result)

        return True


    #
    # создать конвейер
    #

    # недекорируемые задачи
    t_versions = [cmd(task_id='python_version', bash_command='python --version',),
                  cmd(task_id='airflow_version', bash_command='airflow version',)]

    t_check = empty(task_id='check') # типа что-то здесь проверяем

    # передача результата выполнения в качестве параметра следующей задаче
    # создаёт связь в конвейере, поэтому приходится передавать данные
    # либо через XCom, либо через файловую систему
    t_ranks = load_ranks('https://cbrf.py/api/ranks')
    t_goods = load_goods('https://moex.py/api/goods')

    # здесь ветвление
    t_decision = branch(task_id='decision', python_callable=decision)

    t_usd = transform_by_usd()
    t_eur = transform_by_eur()

    t_store = store()
    t_list = cmd(task_id='list', bash_command='pwd; ls -lh;')

    # финальная цепочка
    t_versions >> t_check >> [t_ranks, t_goods] \
        >> t_decision >> [t_usd, t_eur] >> t_store >> t_list


# зарегистрировать себя
dag = pipeline()
