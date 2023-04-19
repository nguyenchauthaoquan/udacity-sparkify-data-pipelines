from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 connection_id=None,
                 tables=None,
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)

        if connection_id is None:
            connection_id = {
                "credentials": "",
                "redshift": ""
            }

        if tables is None:
            tables = []

        # Map params here
        # Example:
        self.connection_id = connection_id
        self.tables = tables

    def execute(self, context):
        postgres_hook = PostgresHook(self.connection_id["redshift"])

        for table in self.tables:
            records = postgres_hook.get_records("SELECT COUNT(*) FROM {table}".format(table=table))

            if len(records) < 1 or len(records[0]) < 1 or records[0][0] < 1:
                self.log.info("Data quality check failed. {table} returned no results".format(table=table))

            self.log.info("Data quality on table {table} check passed with {records} records".format(table=table,
                                                                                                     records=records[0][
                                                                                                         0]))
