from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries

class LoadFactOperator(BaseOperator):

    """
    Custom operator that loads data from Redshift staging tables to final fact table.
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                redshift_conn_id = '',
                append_mode = True,
                fct_table = '',
                 *args, **kwargs):

        """
        Constructs all the necessary attributes for the LoadFactOperator object.

        Parameters
        ----------
            redshift_conn_id (str) - name of the Airflow connection to connect to Redshift
            append_mode (bool) - if ‘True’, appends to table rather than clearing table before adding records
            fct_table (str) - name of final fact table in Redshift where data will be loaded
        """

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.fct_table = fct_table
        self.append_mode = append_mode


    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        if self.append_mode == True:
            self.log.info('Appending data to destination redshift fact table')
            insertion_query = eval('SqlQueries.'+ '{}_table_insert'.format(self.fct_table))
            fct_query = 'INSERT INTO {} {};'.format(self.fct_table, insertion_query)
        else:
            self.log.info('Clearing data from destination redshift table')
            redshift_hook.run('TRUNCATE {};'.format(self.fct_table))
            self.log.info('Loading data into destination redshift fact table')
            insertion_query = eval('SqlQueries.'+ '{}_table_insert'.format(self.fct_table))
            fct_query = 'INSERT INTO {} {};'.format(self.fct_table, insertion_query)

        redshift_hook.run(fct_query)