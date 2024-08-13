from typing import Any

from airflow.contrib.hooks.aws_hook import AwsHook  # type: ignore
from airflow.hooks.postgres_hook import PostgresHook  # type: ignore
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = "#358140"

    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        TIMEFORMAT as 'epochmillisecs'
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
        FORMAT AS JSON '{}'
    """

    @apply_defaults
    def __init__(
        self: Any,
        redshift_conn_id: str = "",
        aws_credentials_id: str = "",
        table: str = "",
        s3_bucket: str = "",
        s3_key: str = "",
        json_path: str = "auto",
        create_sql: str = "",
        *args: Any,
        **kwargs: Any,
    ) -> None:

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.create_sql = create_sql

    def execute(self: Any, context: Any) -> None:
        # Create connections to redshift and s3
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Clear data from the staging table in Redshift
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DROP TABLE IF EXISTS public.{}".format(self.table))

        # Create the staging table in Redshift
        self.log.info("Creating the staging table in Redshift")
        redshift.run(self.create_sql)

        # Copy data from S3 to Redshift
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        if self.json_path != "auto":
            json = "s3://{}/{}".format(self.s3_bucket, self.json_path)
        else:
            json = "auto"
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table, s3_path, credentials.access_key, credentials.secret_key, json
        )
        redshift.run(formatted_sql)
