import logging
from airflow.models.baseoperator import BaseOperator
from airflow.exceptions import AirflowException
import requests
import json
import csv


class NikitaResidentsCountOperator(BaseOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def execute(self, context):
        locations = []
        res = requests.get("https://rickandmortyapi.com/api/location")
        if res.status_code != 200:
            raise AirflowException("Error in load from Rick&Morty API")
        res = json.loads(res.content)
        locations += [
            {
                "id": i["id"],
                "name": i["name"],
                "type": i["type"],
                "dimension": i["dimension"],
                "resident_cnt": len(i["residents"]),
            }
            for i in res["results"]
        ]
        while res["info"]["next"]:
            res = requests.get(res["info"]["next"])
            if res.status_code != 200:
                raise AirflowException("Error in load from Rick&Morty API")
            res = json.loads(res.content)
            locations += [
                {
                    "id": i["id"],
                    "name": i["name"],
                    "type": i["type"],
                    "dimension": i["dimension"],
                    "resident_cnt": len(i["residents"]),
                }
                for i in res["results"]
            ]
        locations = sorted(locations, key=lambda x: x["resident_cnt"], reverse=True)[:3]
        logging.info("Got locations")
        with open("/tmp/locations.csv", "w") as csv_file:
            writer = csv.writer(csv_file, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for l in locations:
                writer.writerow([l["id"], l["name"], l["type"], l["dimension"], l["resident_cnt"]])
                logging.info(l)
