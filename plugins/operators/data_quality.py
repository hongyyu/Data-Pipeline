from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """Check data quality to avoid NULL columns

    :param redshift_conn_id: get redshift hook stored at Airflow
    :type redshift_conn_id: String
    :param table_list: list of table names you want to check data quality
    :type table_list: List
    :param expected_result: number of records are expected to see as result
    :type expected_result: Integer
    """
    # Task background color
    ui_color = '#89DA59'
    # Template SQL to compute number of records in tables
    count_sql = """
        SELECT COUNT(*) AS cnt
        FROM {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table_list=[],
                 expected_result=1,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_list = table_list
        self.expected_result = expected_result

    def execute(self, context):
        # Get redshift hook
        self.log.info('Data Quality Check Start...')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # For each table, check number of rows
        for table in self.table_list:
            self.log.info(f'Running test on table {table}')
            check_num_sql = DataQualityOperator.count_sql.format(table)
            res = redshift.get_records(check_num_sql)

            if len(res) < self.expected_result or len(res[0]) < self.expected_result:
                raise ValueError(f"Data quality check failed. {table} returned no results")

            num_records = res[0][0]
            if num_records < self.expected_result:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")

            self.log.info(f"Data quality on table {table} check passed with {num_records} records")
