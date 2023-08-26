from i_derkach_14_plugins.hooks import RickMortytHook
from datetime import datetime, timedelta
from airflow.models import BaseOperator

class TopLocationsOperator(BaseOperator):
    """ Оператор получения самых населенных локаций
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get(self, conn_id:str, endpoint:str, **kwargs) -> list: # pd.core.frame.DataFrame:
        """ Рекурсивный запрос данных
            При наличии в ответе ссылки на следующую страницу повторить вызов функции

        >>> ... url ~ (str) - адрес запроса
        >>> return (list|pd.core.frame.DataFrame) - итоговый список/датафрейм
        """

        api_hook = RickMortytHook(conn_id)
        data = api_hook.get(method = 'GET', endpoint=endpoint)
        if not data['results']: return [] ### если страница пуста

        # df = pd.DataFrame( ### не уверен, что есть pandas
        #     data=[(*x.values(), len(set(x['residents']))) for x in data['results']],
        #     columns=[*data['results'][0].keys(), 'residents_cnt']
        # ).drop(columns=['residents', 'url', 'created'])
        columns = [*list(data['results'][0].keys())[:4], 'residents_cnt']
        df = [
            dict(zip(
                columns, (*list(x.values())[:4], len(set(x['residents'])))
            )) for x in data['results']
        ]

        return df if not data['info']['next'] else \
            df + self.get(conn_id=conn_id, endpoint=data['info']['next'])
            # pd.concat([df, get_next(data['info']['next'])], ignore_index=True)

            ### если требуется переход, вернуть результат рекурсивного вызова

    def execute(self, context, top_count:int=3) -> list: # pd.core.frame.DataFrame:
        data = self.get(conn_id='dina_ram', endpoint='api/location')
        context['ti'].xcom_push(
            key='i-derkach-14-05-toplocations',
            value=sorted(data, key=lambda x: x['residents_cnt'], reverse=True)[:top_count]
                # df.sort_values(by='residents_cnt', ascending=False)[:3]
        )
