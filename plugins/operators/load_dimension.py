from typing import Any

from airflow.hooks.postgres_hook import PostgresHook  # type: ignore
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = "#80BD9E"

    @apply_defaults
    def __init__(
        self: Any,
        redshift_conn_id: str = "",
        table: str = "",
        create_sql: str = "",
        insert_sql: str = "",
        is_truncating: bool = True,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.create_sql = create_sql
        self.insert_sql = insert_sql
        self.is_truncating = is_truncating

    def execute(self: Any, context: Any) -> None:
        # Create connections to redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Create the dimension table if not existed in Redshift
        self.log.info("Creating the dimension table in Redshift")
        self.log.info(self.create_sql)
        redshift.run(self.create_sql)

        if self.is_truncating:
            self.log.info(f"Truncating the dimension table: {self.table}")
            redshift.run(f"TRUNCATE TABLE {self.table}")

        # Insert data into the dimension table
        self.log.info("Inserting data into the dimension table")
        redshift.run(self.insert_sql)
