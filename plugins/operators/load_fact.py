from typing import Any

from airflow.hooks.postgres_hook import PostgresHook  # type: ignore
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = "#F98866"

    @apply_defaults
    def __init__(
        self: Any,
        redshift_conn_id: str = "",
        table: str = "",
        create_sql: str = "",
        insert_sql: str = "",
        mode: str = "append",
        *args: Any,
        **kwargs: Any,
    ) -> None:

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.create_sql = create_sql
        self.insert_sql = insert_sql
        self.mode = mode

    def execute(self: Any, context: Any) -> None:
        # Create connections to redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Fact tables are usually so massive that they should only allow append type functionality
        if self.mode != "append":
            self.log.info(f"Truncating the fact table: {self.table}")
            redshift.run(f"TRUNCATE TABLE {self.table}")

        # Create the fact table if not existed in Redshift
        self.log.info("Creating the fact table in Redshift")
        self.log.info(self.create_sql)
        redshift.run(self.create_sql)

        # Insert data into the fact table
        self.log.info("Inserting data into the fact table")
        redshift.run(self.insert_sql)
