from airflow.models import BaseOperator
from airflow import AirflowException
from airflow.hooks.postgres_hook import PostgresHook
import logging
import requests
import pandas as pd


class ed_kuzhamuratov_17_RamTop3LocationsOperator(BaseOperator):
    """
    Find top 3 locations with most residents and upload to Greenplum table
    """

    ui_color = "#e0ffff"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def f_ram_get_pages(self, api_url):
        """
        Get count of page in API
        :param api_url
        :return: page count
        """
        r = requests.get(api_url)
        if r.status_code == 200:
            # logging.info("SUCCESS")
            page_count = r.json().get('info').get('pages')
            logging.info(f'page_count = {page_count}')
            return page_count
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

    def f_get_top3_locations(self):
        locations_list = []
        location_url = 'https://rickandmortyapi.com/api/location'
        for page in range(self.f_ram_get_pages(location_url)):
            response = requests.get(location_url + f'/?page={page + 1}')
            if response.status_code == 200:
                #logging.info(f'PAGE {page + 1}')
                for location in response.json()['results']:
                    loc_id = location['id']
                    loc_name = location['name']
                    loc_type = location['type']
                    loc_dimension = location['dimension']
                    loc_residents = location['residents']
                    locations_list.append(
                        {"id": loc_id,
                         "name": loc_name,
                         "type": loc_type,
                         "dimension": loc_dimension,
                         "residents_cnt": len(loc_residents)}
                    )

            else:
                logging.warning("HTTP STATUS {}".format(response.status_code))
                raise AirflowException('Error in load from Rick&Morty API')

        locations_df = pd.DataFrame(locations_list)
        top3locations_df = locations_df.nlargest(3, 'residents_cnt')
        return top3locations_df

    def execute(self, context):
        top3locations_df = self.f_get_top3_locations()

        # Creating table if it doesn't exist and deleting from it
        logging.info("Creating the table if it doesn't exist and clearing it")
        table_sql = """
            CREATE table IF NOT EXISTS "ed-kuzhamuratov-17_ram_location" (
                id int4 NULL,
                name text NULL,
                type text NULL,
                dimension text null,
                resident_cnt int4 NULL
            )
            DISTRIBUTED BY (id);
            
            DELETE FROM "ed-kuzhamuratov-17_ram_location";
        """
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg_hook.run(table_sql, False)

        # Inserting the top 3 locations into the table
        logging.info("Inserting the top 3 locations into the table")
        insert_values = []
        for index, row in top3locations_df.iterrows():
            insert_values.append(
                f'({str(row["id"])}, \'{row["name"]}\', \'{row["type"]}\', \'{row["dimension"]}\', {str(row["residents_cnt"])})'
            )
        insert_stmt = f'INSERT INTO "ed-kuzhamuratov-17_ram_location" VALUES {", ".join(insert_values)}'
        pg_hook.run(insert_stmt, False)

        # Printing the table's content
        logging.info("Printing the table's content")
        conn = pg_hook.get_conn()  # get the connection
        cursor = conn.cursor()  # creating a cursor
        cursor.execute(f'SELECT * FROM "ed-kuzhamuratov-17_ram_location"')  # run the parameterized sql
        records = cursor.fetchall()
        print(f"Total rows are: {len(records)}. Printing each row:", )
        for row in records:
            logging.info(row)
