#-----------------------------------------------------------------------------
# Прочитать заголовок заказа по параметру id = [день недели]
# Записать прочитанное в XCom
# postgres_conn_id='conn_greenplum'
#-----------------------------------------------------------------------------

from airflow import DAG
import pendulum

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook


dag = DAG(
    dag_id="e-semirekov_read_article_head",
    start_date=pendulum.datetime(2022, 3, 1, tz="UTC"),
    end_date=pendulum.datetime(2022, 3, 14, tz="UTC"),
    schedule_interval='0 0 * * 1-6', # Ежедневно, кроме воскресенья
    tags=['semirekov'],
    catchup=True
)   

#----------------------------------------------------------------------------
# Конечно все можно было бы сделаь одной функцией, но тогда она содержала бы
# несколько действий, которые требуют своей отдельной логики.
#----------------------------------------------------------------------------

def read_article_head(article_id):
# Прочитать заголовок заказа по параметру [article_id]

    # Подключение к источнику данных
    pg_hook = PostgresHook('conn_greenplum')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    # Выполнение SQL-запроса
    cursor.execute(f'SELECT heading FROM articles WHERE id = {article_id}')    
    # Считываем и возвращаем полученный результат 
    return cursor.fetchone()[0] 

def getweekDay(execution_date):
    # День недели по дате выполнения    
    wday = execution_date.weekday() 
    # Нумерация в источнике данных начинается с 1, а не с нуля, поэтому дабавляем единичку
    wday += 1 

    return wday
    
def read_article_by_weekday(execution_date):
    wday = getweekDay(execution_date)
    # Получить данные из источника
    result = read_article_head(wday) 
    # Отформатировать результат (могла бы быть отдельная функция, при более сложном форматировании)
    result = f'{ execution_date }: {result}'
    #Записать результат в XCom через встроенный механизм возврата значения из функции
    return result

    

readArticleHead = PythonOperator(task_id='readArticleHead', python_callable=read_article_by_weekday, dag=dag)

readArticleHead 
