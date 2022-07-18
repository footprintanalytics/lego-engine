
from defi360.validation.basic_validate.address_format_validate import AddressFormatValidate
from defi360.validation.basic_validate.field_not_null_validate import FieldNotNullValidate
from defi360.validation.basic_validate.abnormal_number_validate import AbnormalNumberValidate
from defi360.validation.basic_validate.negative_number_validate import NegativeNumberValidate
from defi360.validation.basic_validate.black_hole_address_validate import BlackHoleAddressValidate
from defi360.validation.basic_validate.data_continuity_validate import DataContinuityValidate
import requests
import pydash
import datetime


class DeFi360Validation:
    project_id = "gaia-dao"
    dataset = "gaia_dao"
    hasura_url = "https://hasura-prod.internal.footprint.network/v1/graphql"

    def get_validate_config(self, params: dict):
        project = params["project"]
        validate_table = "{project_id}.{dataset}.{project}".format(
            project_id=self.project_id,
            dataset=self.dataset,
            project=project
        )

        return [
            {
                "cls": AddressFormatValidate,
                "config": {
                    "validate_field": [
                        "contract_address",
                        "operator",
                        "token_address"
                    ],
                    "validate_table": validate_table,
                    "project": project
                }
            },
            {
                "cls": FieldNotNullValidate,
                "config": {
                    "validate_field": [
                        "project",
                        "chain",
                        "protocol_id",
                        "block_number",
                        "block_timestamp",
                        "transaction_hash",
                        "log_index",
                        "contract_address",
                        "operator",
                        "token_address",
                        "token_amount_raw",
                        "pool_id",
                        "name",
                        "version"
                    ],
                    "validate_table": validate_table,
                    "project": project
                }
            },
            {
                "cls": AbnormalNumberValidate,
                "config": {
                    "validate_field": [
                        "usd_value"
                    ],
                    "validate_table": validate_table,
                    "project": project
                }
            },
            {
                "cls": NegativeNumberValidate,
                "config": {
                    "validate_field": [
                        "token_amount",
                        "token_amount_raw"
                    ],
                    "validate_table": validate_table,
                    "project": project
                }
            },
            {
                "cls": BlackHoleAddressValidate,
                "config": {
                    "validate_field": [
                        "operator"
                    ],
                    "validate_table": validate_table,
                    "project": project
                }
            },
            {
                "cls": DataContinuityValidate,
                "config": {
                    "validate_field": [],
                    "validate_table": validate_table,
                    "project": project
                }
            }
        ]

    def get_protocol_from_hasura(self):
        result = requests.post(
            url=self.hasura_url,
            json={
                "query": "{\
                    indicator_protocol {\
                        id\
                        name\
                    }\
                }"
            }
        )
        return pydash.get(result.json(), 'data.indicator_protocol', [])

    def validate(self):
        all_protocol = self.get_protocol_from_hasura()
        all_project = pydash.map_(all_protocol, lambda protocol: protocol["name"] + '_' + protocol["id"])
        print('需要校验的平台：', all_project)

        for project in all_project:
            validate_table = f"{self.project_id}.{self.dataset}.{project}"
            configs = self.get_validate_config({
                "project": project
            })
            for config in configs:
                config["cls"](config["config"]).validate()

    def airflow_steps(self):
        return [
            self.validate
        ]

    @staticmethod
    def airflow_dag_params():
        dag_params = {
            "dag_id": "DeFi360_validation_dag",
            "catchup": False,
            "schedule_interval": '30 6 * * *',
            "description": "DeFi360_validation_dag",
            "default_args": {
                'owner': 'airflow',
                'depends_on_past': False,
                'retries': 1,
                'retry_delay': datetime.timedelta(minutes=5),
                'start_date': datetime.datetime(2022, 1, 1)
            },
            "dagrun_timeout": datetime.timedelta(days=30)
        }
        print('dag_params', dag_params)
        return dag_params


if __name__ == '__main__':
    deFi360Validation = DeFi360Validation()
    deFi360Validation.validate()
