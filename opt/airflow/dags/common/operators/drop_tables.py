from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class DropTablesOperator(BaseOperator):
    ui_color = '#358140'

    def __init__(self,
                 connection_id="redshift",
                 tables=None,
                 is_deleted=False,
                 *args, **kwargs):
        super(DropTablesOperator, self).__init__(*args, **kwargs)

        if tables is None:
            tables = []

        self.connection_id = connection_id
        self.tables = tables
        self.is_deleted = len(self.tables) > 0 or is_deleted

    def execute(self, context):
        if self.is_deleted:
            postgres_hook = PostgresHook(postgres_conn_id=self.connection_id)
            self.log.info("Dropping all tables including {}".format(self.tables))

            for table in self.tables:
                postgres_hook.run("DROP TABLE {};".format(table))
        else:
            self.log.info("No tables to delete")
