from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    """Insert data from staging tables into dimension tables

    :param redshift_conn_id: get redshift hook stored at Airflow
    :type redshift_conn_id: String
    :param table: fact table name in order for inserting
    :type table: String
    :param sql: SQL task to process appropriate data
    :type sql: String
    :param is_truncate: True if want to delete previous data, otherwise False
    :type is_truncate: Boolean
    """
    # Task background color
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 sql='',
                 is_truncate=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.is_truncate = is_truncate

    def execute(self, context):
        # Get redshift hook
        self.log.info('Get Redshift Hook')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # If True, delete previous loaded data
        if self.is_truncate:
            self.log.info(f'Truncate table {self.table} for reloading!')
            redshift.run(f'TRUNCATE TABLE {self.table}')

        # Load data into dimension tables
        self.log.info(f'Start to Load dimension table {self.table}!')
        table_insert = f"""
            INSERT INTO {self.table}
            {self.sql}
        """
        redshift.run(table_insert)
