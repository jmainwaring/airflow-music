from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    
    """
    Custom operator that loads raw json data from S3 to a Redshift table.
    """
    
    ui_color = '#358140'

    stage_sql_template = """
    COPY {}
    FROM 's3://{}/{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    JSON '{}'
    REGION 'us-west-2'
    """

    @apply_defaults
    def __init__(self,
                redshift_conn_id = '',
                aws_credentials_id = '',
                staging_table = '',
                s3_bucket = '',
                s3_directory = '',
                delimiter = ',',
                json_formatting = 'auto',
                ignore_headers = 1,
                 *args, **kwargs):

        """
        Constructs all the necessary attributes for the StageToRedshiftOperator object.

        Parameters
        ----------
            redshift_conn_id (str) - name of the Airflow connection to connect to Redshift
            aws_credentials_id (str) - name of the Airflow connection to connect to S3
            staging_table (str) - name of staging table in Redshift where data will be loaded
            s3_bucket (str) - S3 bucket where original data resides
            s3_directory (str) - S3 directory within bucket where original data resides
        """

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id 
        self.aws_credentials_id = aws_credentials_id
        self.staging_table = staging_table
        self.s3_bucket = s3_bucket
        self.s3_directory = s3_directory
        self.delimiter = delimiter
        self.json_formatting = json_formatting
        self.ignore_headers = ignore_headers


    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Clearing data from destination redshift table')
        redshift_hook.run('TRUNCATE {};'.format(self.staging_table))

        self.log.info('Copying data from S3 to Redshift')

        formatted_stage_sql = StageToRedshiftOperator.stage_sql_template.format(
            self.staging_table,
            self.s3_bucket,
            self.s3_directory,
            credentials.access_key,
            credentials.secret_key,
            self.json_formatting
        )   

        redshift_hook.run(formatted_stage_sql)