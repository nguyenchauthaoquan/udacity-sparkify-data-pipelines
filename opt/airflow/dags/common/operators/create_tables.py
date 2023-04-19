from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class CreateTablesOperator(BaseOperator):
    ui_color = '#358140'

    def __init__(self,
                 connection_id="redshift",
                 sql_file="",
                 *args, **kwargs):
        super(CreateTablesOperator, self).__init__(*args, **kwargs)

        self.connection_id = connection_id
        self.sql_file = sql_file

    def execute(self, context):
        postgres_hook = PostgresHook(postgres_conn_id=self.connection_id)

        sql_file = open(self.sql_file, "r")
        sql_commands = sql_file.read().split(";")

        sql_file.close()

        for command in sql_commands:
            if command.rstrip() != '':
                postgres_hook.run(command)