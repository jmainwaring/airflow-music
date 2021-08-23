from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries

class LoadDimensionOperator(BaseOperator):

    """
    Custom operator that loads data from Redshift staging tables to a dimension table.
    """

    ui_color = '#80BD9E'
    
    @apply_defaults
    def __init__(self,
                redshift_conn_id = '',
                dim_table = '',
                 *args, **kwargs):

        """
        Constructs all the necessary attributes for the LoadDimensionOperator object.

        Parameters
        ----------
            redshift_conn_id (str) - name of the Airflow connection to connect to Redshift
            dim_table (str) - name of dimension table in Redshift where data will be loaded
        """

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dim_table = dim_table

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Clearing data from destination redshift table')
        redshift_hook.run('TRUNCATE {};'.format(self.dim_table))

        self.log.info('Loading data into dimensional table')
        insertion_var_name = '{}_table_insert'.format(self.dim_table)
        insertion_query = eval('SqlQueries.'+ insertion_var_name)
        dim_query = 'INSERT INTO {} {};'.format(self.dim_table, insertion_query)

        redshift_hook.run(dim_query)