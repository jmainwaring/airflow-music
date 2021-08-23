from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.data_quality_checks import DataQualityChecks

class DataQualityOperator(BaseOperator):

    """
    Custom operator that performs a number of pre-defined data quality checks on a table. One limitation 
    under active development is that the current 'desired_checks' dictionary approach prevents you from 
    running the same check on multiple different columns of a table without separate operators.
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                redshift_conn_id = '',
                table = '',
                desired_checks = {}, 
                 *args, **kwargs):

        """
        Constructs all the necessary attributes for the DataQualityOperator object.

        Parameters
        ----------
            redshift_conn_id (str) - name of the Airflow connection to connect to Redshift
            table (str) - the table to run quality checks on
            desired_checks (dict) - dictionary where each key is the name of a check to perform, and the value 
            contains a dictionary of additional parameters if required by that check, like the column name
        """

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.desired_checks = desired_checks

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        for check in self.desired_checks.keys():
            args_dict = {'db_hook': redshift_hook, 'table': self.table}
            self.log.info('Initial args dict: ' + str(args_dict))
            
            # Adds the additional arguments passed in to the args_dict, like column name
            args_dict = {**args_dict, **self.desired_checks[check]}
            db_hook = args_dict['db_hook']
            table = args_dict['table']
            self.log.info('Combined args dict: ' + str(args_dict))

            # Creates reference to the correct data quality function
            function_name = DataQualityChecks.mapping_dict[check]
            self.log.info('Function name: ' + str(function_name))
            
            # Adds column variable if data quality check requires it, and executes the check
            if 'column' in args_dict.keys():
                column = args_dict['column']
                test_result = function_name(db_hook=db_hook, table=table, column=column)
            elif 'lower_bound' in args_dict.keys():
                lower_bound, upper_bound = args_dict['lower_bound'], args_dict['upper_bound']
                test_result = function_name(db_hook=db_hook, table=table, lower_bound=lower_bound, upper_bound=upper_bound)
            else:
                test_result = function_name(db_hook=db_hook, table=table)

            if test_result:
                self.log.info(check + ' test passed')
            else:
                raise AssertionError (check + ' test failed')