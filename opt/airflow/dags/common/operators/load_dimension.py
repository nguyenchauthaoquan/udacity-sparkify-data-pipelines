from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 connection_id="",
                 table="",
                 sql_query="",
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.connection_id = connection_id
        self.table = table
        self.sql_query = sql_query

    def execute(self, context):
        postgres_hook = PostgresHook(postgres_conn_id=self.connection_id)
        self.log.info('Loading data into {table} dimension table'.format(table = self.table))

        postgres_hook.run("""
            INSERT INTO {table}
            {sql_query};
        """.format(
            table = self.table,
            sql_query = self.sql_query
        ))

        self.log.info('Loaded data into {table} dimension table'.format(table = self.table))

