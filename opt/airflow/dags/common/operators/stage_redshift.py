from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    sql = """
        COPY {table}
        FROM '{path}'
        ACCESS_KEY_ID '{access_key_id}'
        SECRET_ACCESS_KEY '{secret_key_id}'
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
        REGION AS '{region}'
        FORMAT AS JSON 'auto'
        TIMEFORMAT AS 'auto';
    """

    def __init__(self,
                 connection_id=None,
                 table="",
                 s3=None,
                 region = "",
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        if connection_id is None:
            connection_id = {
                "credentials": "",
                "redshift": ""
            }

        if s3 is None:
            s3 = {
                "bucket_name": "",
                "prefix": ""
            }

        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.connection_id = connection_id
        self.table = table
        self.s3 = s3
        self.region = region

    def execute(self, context):
        credentials_hook = AwsBaseHook(self.connection_id["credentials"])
        credentials = credentials_hook.get_credentials()
        postgres_hook = PostgresHook(postgres_conn_id=self.connection_id["redshift"])
        s3_path = "s3://{}/{}".format(self.s3["bucket_name"], self.s3["prefix"])

        self.log.info("Copying data from {} to Redshift".format(s3_path))

        postgres_hook.run("""
            COPY {table}
            FROM '{path}'
            ACCESS_KEY_ID '{access_key_id}'
            SECRET_ACCESS_KEY '{secret_key_id}'
            TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
            REGION AS '{region}'
            FORMAT AS JSON 'auto'
            TIMEFORMAT AS 'auto';
        """.format(
            table = self.table,
            path = s3_path,
            access_key_id = credentials.access_key,
            secret_key_id = credentials.secret_key,
            region = self.region,
        ))

        self.log.info("Copying data from {} to Redshift is finished".format(s3_path))
