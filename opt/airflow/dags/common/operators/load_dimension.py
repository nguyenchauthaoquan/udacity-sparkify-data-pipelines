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
                 is_truncated=True,
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.connection_id = connection_id
        self.table = table
        self.sql_query = sql_query
        self.is_truncated = is_truncated

    def execute(self, context):
        postgres_hook = PostgresHook(postgres_conn_id=self.connection_id)
        self.log.info('Loading data into {table} dimension table'.format(table = self.table))

        if self.is_truncated:
            self.log.info("Clearing data from {} table...".format(self.table))
            postgres_hook.run("TRUNCATE TABLE {}".format(self.table))
            self.log.info("Cleared data from {} table successfully".format(self.table))

        postgres_hook.run("""
            INSERT INTO {table}
            {sql_query};
        """.format(
            table = self.table,
            sql_query = self.sql_query
        ))

        self.log.info('Loaded data into {table} dimension table'.format(table = self.table))
