from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 connection_id=None,
                 dq_checks=None,
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)

        if connection_id is None:
            connection_id = {
                "credentials": "",
                "redshift": ""
            }

        if dq_checks is None:
            dq_checks = []
        # Map params here
        # Example:
        self.connection_id = connection_id
        self.dq_checks = dq_checks

    def execute(self, context):
        postgres_hook = PostgresHook(self.connection_id["redshift"])

        if len(self.dq_checks) > 0:
            self.log.info("Beginning to check data quality")
            for i, dq_check in enumerate(self.dq_checks):
                if dq_check["test_sql"] is not None and len(dq_check["test_sql"]) > 0:
                    self.log.info("Checking {} ".format(i))
                    records = postgres_hook.get_records(dq_check['test_sql'])

                    if dq_check['expected_results'] != records[0][0]:
                        raise ValueError("Data quality check #{} failed. Expected: {}, actual: {}".format(
                            i,
                            dq_check['expected_results'],
                            records[0][0]))
                    else:
                        self.log.info("Data quality check #{} success. Expected: {}, actual: {}".format(
                            i,
                            dq_check['expected_results'],
                            records[0][0]))
        else:
            self.log.info("Skipped to check data quality")
