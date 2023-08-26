from airflow import DAG
import pendulum
import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    "start_date": pendulum.datetime(2022, 3, 1, tz="UTC"), #указываем дату исполнения с учётом времени сервера
    "end_date": pendulum.datetime(2022, 3, 14, tz="UTC"),  #указываем дату исполнения с учётом времени сервера
    'owner': 'a-zaharova',  #владелец дага
    'poke_interval': 30
}

with DAG("a-zaharova_d_latest_version",#название дага
         schedule_interval='@daily', #выполнение каждый день в полночь
         default_args=DEFAULT_ARGS, #загрузка аргументов, описанных выше
         max_active_runs=1,
         tags=['a-zaharova', 'articles', 'select']
         ) as dag:

    #пустые таски, можно использовать для разделения веток
    dummy = DummyOperator(task_id="dummy")
    chapter_one = DummyOperator(task_id="chapter_one")

    #оператор, который исполняет код в терминале
    # сначала указываем название таска
    # так-как код выполняется в терминале, а не в python
    # сразу пишем запрос, который нужно выполнить в терминале echo {{ds}}
    echo_a = BashOperator( #имена передаваемых переменных обязательно должны быть такими
        task_id='echo_a', #название таска
        bash_command='echo {{ds}}' #код который он должен исполнить
    )
    # функция, которая выводит дату выполнения. Python не знает об этой дате. Поэтому её передаёт таск
    # так как мы передали этой функции дату выполнения
    # нужно её загрузить в аргументы today_date_func(execution_date)
    # далее уже с помощью print её выводим)
    def today_date_func(execution_date): #функция, которая выводит дату выполнения. Python не знает об этой дате. Поэтому её передаёт таск
        print(execution_date) #обязательно в аргументах указывается название получаемой из таска функции

    #опрератор, который выполняет код на python в функции today_date_func.
    # служит, чтобы отобразить текущую дату с помощью python
    # поэтому мы передаём дату из таска, так-как только он её знает  execution_date = {{ds}}
    # всё это будет выполнено в функции выше today_date_func
    today_date = PythonOperator( #названия передаваемых переменных важно сохранять
        task_id='today_date', #название таска
        python_callable=today_date_func, #функция, которую выполняем
        op_kwargs=({'execution_date': '{{ds}}'}), #передаваемое значение (переменная execution_date и значение {{ds}}
    )
    # ветвление. тут мы один раз запускаем от dummy, потом используем разделитель chapter_one для красоты и выполняем одновременно ещё два таска echo_a
    # выполняем dummy, чтобы разделить ветки
    # выполняем chapter_one, чтобы отделить эту ветку от следующей и не было каши в ветках
    # выполняем сразу два таска [echo_a, today_date], так-как они друг от друга не зависят
    dummy >> chapter_one >> [echo_a, today_date]


    # функция для ограничения выполнения кода по воскресеньям
    # обязательно указываем аргументы execution_date, **kwargs, мы с ними работаем
    # далее приводим дату, которую передали из таска в execution_date к виду даты в переменную exec_day
    # и подфункцией weekday() запрашиваем какой день недели
    # далее отправляем в функцию, которая должна принять переменную weekday, содержащую (exec_day+1)
    # и выполняем логическое выражение если день выполнения не воскресенье, то true,  иначе false
    # мы отсюда передаём(xcom_push) переменную weekday, содержащую числовое значение дня недели, начиная с 1
    # так как мы значение передаём, то тоже указываем в аргументах, что будем использовать функцию **kwargs
    def anti_sunday_func(execution_date, **kwargs): # также указываем execution_date, которую передали из таска
        exec_day = datetime.datetime.strptime(execution_date, '%Y-%m-%d').weekday() #чтобы определить дату выполнения в формат даты и
        kwargs['ti'].xcom_push(value=(exec_day+1), key="weekday")#тут передаём переменную по xcom              определить(weekday()) день недели
        return exec_day != 6 #логическое выражение. если день выполнения не воскресенье, то true,  иначе false

    # функция для выполнения запроса в базу данных и отображения полученного заголовка новости по текущему дню недели
    #сначала указываем аргументы функции, с которыми мы будем работать
    # тут мы получаем (pull) значение weekday из предыдущей функции, обязательно указываем название таска, который организует работу xcom
    # выполняем соединение по хуку
    # и сам запрос
    # далее выводим в переменную one_string значение заголовка нужной новости 
    # и с помощью print выводим сначала дату, потом двоеточие и сам заголовок
    def select_heading_func(execution_date, **kwargs): #обязательно указываем **kwargs и дату выполнения, переданную таском, так-как работаем с ними
        weekday = kwargs['ti'].xcom_pull(task_ids='anti_sunday', key='weekday') #также указываем ключ, по которому будем переменную получать
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("cursor_date")  # и именованный (необязательно) курсор
        cursor.execute('SELECT heading FROM articles WHERE id = '+str(weekday))  # исполняем sql
        one_string = cursor.fetchone()[0]  # если вернулось единственное значение
        print(execution_date, ': ', one_string) # объёдиняем строку. выводим дату выполнения, двоеточие и заголовок heading в лог

    # таск на выполнение функции для определения воскресенье ли сегодня
    # в операторе ниже мы обызательно указываем название таска
    # в функции выше мы использовали его для получения переменной weekday
    # далее указываем функцию, которая будет исполнять этот таск
    # и передаваемое значение даты выполнения execution_date
    # это ShortCircuitOperator, значит в функции, которую он выполнит важно выдать true или false
    # чтобы определить выполнять следующие таски или нет
    # в воскресенье функция вернёт false
    # и следующие таски (select_heading) не выполняются
    anti_sunday = ShortCircuitOperator(#названия передаваемых переменных важно сохранять
        task_id='anti_sunday', #название таска. в предыдущей функции мы его использовали, так-как именно этот таск организовал передачу **kwargs
        python_callable=anti_sunday_func,#выполняемая функция
        op_kwargs={'execution_date': '{{ds}}'}#передаваемое значение, для проверки на воскресенье
    )
    # если сегодня не воскресенье, то выполняется этот таск, иначе пропускается 
    # служит для определения фунции, которая выберет из базы данных заголовок статьи, которая должна быть опубликована в этот день недели
    # для отображения в консоли даты выполнения таска мы передаём значение execution_date = {{ ds }} скрипту
    # выполняется по этому таску скрипт select_heading_func
    select_heading = PythonOperator(#названия передаваемых переменных важно сохранять
        task_id='select_heading',#название таска select_heading
        python_callable=select_heading_func,#функция, которая выполняется select_heading_func
        op_kwargs={"execution_date": "{{ ds }}"} #то, что мы передаём скрипту на Python execution_date = {{ ds }}
    )
    
# мы снова обращаемся к dummy, чтобы и эта ветка была от этого оператора
# далее выполняем таск anti_sunday, чтобы проверить, нужно ли выполнить следующий (если это не воскресенье)
# далее если выполняем, то select_heading выполнит запрос к базе данных и отобразит нужный заголовок новости
    dummy >> anti_sunday >> select_heading
