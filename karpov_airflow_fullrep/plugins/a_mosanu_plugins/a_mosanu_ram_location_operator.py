import logging
import pandas as pd
from airflow.models import BaseOperator
from airflow.hooks.http_hook import HttpHook
from airflow.hooks.postgres_hook import PostgresHook


class RMHook(HttpHook):
    """
    Interact with Rick&Morty API
    """
    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def get_loc_page_count(self):
        """Returns count of page in API"""
        return self.run('api/location').json()['info']['pages']

    def json_res_location_page(self, page_num) -> dict:
        """Returns count of page in API"""
        return self.run(f'api/location/?page={page_num}').json()['results']

class andr_ShowTopLocationsOperator(BaseOperator):
    """
    Extract top 3 locations with most residents
    """
    ui_color = "#c7ffe9"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def nr_residents_on_page(self, result_json: list) -> pd.DataFrame:
        """
        Get count of dead or alive in one page of character
        """
        loc_df = pd.DataFrame(columns=['id', 'name', 'type', 'dimension', 'resident_cnt'])
        for result in result_json:
            row = {
                'id': result['id'],
                'name': result['name'],
                'type': result['type'],
                'dimension': result['dimension'],
                'resident_cnt': len(result['residents'])
            }
            loc_df.loc[len(loc_df)] = row
        return loc_df

    def execute(self, context):
        """
        Logging count of dead or alive in Rick&Morty
        """
        hook = RMHook('dina_ram')
        df = pd.DataFrame()
        for page in range(hook.get_loc_page_count()):
            page = self.nr_residents_on_page(hook.json_res_location_page(str(page + 1)))
            df = df.append(page)
            df = df.sort_values(by="resident_cnt", ascending=False).head(3)
        logging.info(df)

        #Convert this dataframe to string
        output_string = ""
        for _, row in df.iterrows():
            location = (row['id'], row['name'], row['type'], row['dimension'], row['resident_cnt'])
            output_string += f"{location},"
        output_string = output_string.rstrip(",")  # Remove the trailing comma

        context['ti'].xcom_push(key='written_dataframe', value=output_string)

class WriteDataFrameToGreenplumOperator(BaseOperator):
    def __init__(self,  postgres_conn_id, pull_task_id, xcom_push=True, **kwargs) -> None:
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.pull_task_id = pull_task_id
        self.xcom_push_flag = xcom_push

#For some reasons this code is not working
    # def execute(self, context):
    #     pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
    #
    #     # Pull DataFrame from XCom
    #     df = context['ti'].xcom_pull(task_ids= self.pull_task_id, key='written_dataframe')
    #     data = [tuple(record.values()) for record in df]
    #
    #     # SQL statement to insert records into the table
    #     sql_create_table = """CREATE TABLE if not exists  a_mosanu_ram_location
    #             ( id text, name text,  type text,  dimension text, resident_cnt text) DISTRIBUTED BY (id);"""
    #     sql_write = """
    #         INSERT INTO a_mosanu_ram_location (id, name, type, dimension, resident_cnt)
    #         VALUES (%s, %s, %s, %s, %s) ;
    #     """
    #     with pg_hook.get_conn() as conn:
    #         with conn.cursor() as cursor:
    #             # Write DataFrame to Greenplum table
    #             cursor.execute(sql_create_table)
    #             cursor.executemany(sql_write, data)
    #             conn.commit()
    #     logging.info("---Successfully written into DB---")




