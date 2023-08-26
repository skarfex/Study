from airflow.models import BaseOperator
from s_dipner_plugins.s_dipner_ram_3_top_hook import Sdipner_RaM_location_hook


class Sdipner_RaM_location_operator(BaseOperator):
    ui_color = "#e0ffff"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def execute(self, context) -> None:

        hook = Sdipner_RaM_location_hook('dina_ram')

        locations = []
        for location in hook.generate_schema_of_location():
            row = (
                location['id'],
                location['name'],
                location['type'],
                location['dimension'],
                len(location['residents'])
            )
            self.log.info(list(row))
            locations.append(row)

        top3 = sorted(locations, key=lambda column: column[-1], reverse=True)[:3]
        result = ','.join(map(str, top3))
        self.log.info(f'Top 3 location on residents: {result}')

        return result