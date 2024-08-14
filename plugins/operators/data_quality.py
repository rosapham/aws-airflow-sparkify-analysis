from typing import Any, Dict, List

from airflow.hooks.postgres_hook import PostgresHook  # type: ignore
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = "#89DA59"

    check_null_sql = """
        SELECT COUNT(*)
        FROM {}
        WHERE {} IS NULL
    """

    """
    Parameters:
    redshift_conn_id: str
        The connection id for the redshift connection
    tables: list[str]
        The list of tables to check number of records in each table
    """

    @apply_defaults
    def __init__(
        self: Any,
        redshift_conn_id: str = "",
        tables: List[str] = [],
        entities: Dict[str, str] = {},
        *args: Any,
        **kwargs: Any,
    ):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.entities = entities

    # Check if the number of rows in the table is greater than 0
    def check_rows_greater_than_zero(self: Any, redshift: Any, table: str) -> None:
        records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {table} returned no results")
        if records[0][0] < 1:
            raise ValueError(f"Data quality check failed. {table} contained 0 rows")
        self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")

    # Check if the table contains null values in a specific column
    def check_contain_null(self: Any, redshift: Any, table: str, column: str) -> None:
        query = self.check_null_sql.format(table, column)
        records = redshift.get_records(query)
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {table} returned no results")
        if records[0][0] != 0:
            raise ValueError(
                f"Data quality check failed. {column} in {table} contained NULL values"
            )
        self.log.info(
            f"Data quality on table {table} check passed with {records[0][0]} NULL records in {column}"  # noqa: E501
        )

    def execute(self: Any, context: Any) -> None:
        # Create connections to redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for table in self.tables:
            self.check_rows_greater_than_zero(redshift, table)

        for item in self.entities:
            self.check_contain_null(redshift, item, self.entities[item])
