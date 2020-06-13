from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    """Insert data from staging table into fact tables

    :param redshift_conn_id: get redshift hook stored at Airflow
    :type redshift_conn_id: String
    :param table: fact table name in order for inserting
    :type table: String
    :param sql: SQL task to process appropriate data
    :type sql: String
    """
    # Task background color
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 sql='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        # Get redshift hook
        self.log.info('Get Redshift Hook.')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Insert data into fact table
        self.log.info(f'Start to load fact table {self.table}!')
        table_insert = f"""
            INSERT INTO {self.table}
            {self.sql}
        """
        redshift.run(table_insert)
