from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
import datetime

class StageToRedshiftOperator(BaseOperator):
    
    """Apache Airflow Operator to Copy source data
        from AWS S3 to AWS Redshift DB.
    Keyword arguments:
    * table          -- AWS Redshift target table name
    * redshift_conn_id      -- AWS Redshift connection ID
    * s3_bucket             -- AWS S3 bucket name
    * s3_key                -- AWS S3 key
    * json_path             -- path to JSON data structure
    * file_type             -- source data file format (JSON | CSV)
    * aws_credentials_id    -- AWS S3 credentials
    * execution_date        -- context variable containing date of task's
                                execution date (e.g. 2019-07-19)
    Output:
    * source data is COPIED to AWS Redshift staging tables.
    """
    
    
    ui_color = '#358140'

    # SQL template for JSON input format
    json_copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}'
        COMPUPDATE OFF
    """
    # SQL template for CSV input format
    csv_copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        DELIMITER '{}'
    """
    
    @apply_defaults
    def __init__(self,
                 # Define operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_path="",
                 file_type="",
                 delimiter=",",
                 ignore_headers=1,
                 execution_date="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.file_type = file_type
        self.aws_credentials_id = aws_credentials_id
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.execution_date = execution_date

    def execute(self, context):
        ##self.log.info('StageToRedshiftOperator not implemented yet')
        self.log.info("Initializing Redshift connection...")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        if self.file_type == "json":
            formatted_sql = StageToRedshiftOperator.json_copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.json_path
            )
            #redshift.run(formatted_sql)

        if self.file_type == "csv":
            formatted_sql = StageToRedshiftOperator.csv_copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.ignore_headers,
                self.delimiter
            )
            #redshift.run(formatted_sql)
        else:
            self.log.info('Error, file is neither JSON nor CSV format.')
            pass
        
        # Executing SQL operation
        self.log.info("Running SQL COPY operation...")
        redshift.run(formatted_sql)
        self.log.info("Finished SQL COPY operation.")




