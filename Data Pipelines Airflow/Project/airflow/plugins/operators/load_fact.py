from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """Apache Airflow Operator to load data
        from AWS Redshift staging tables to Fact table.
    Keyword arguments:
    * redshift_conn_id      -- AWS Redshift connection ID
    * table                 -- AWS Redshift target table name
    * query                 -- Query name to be used from SqlQueries.
    * insert                 -- parameter for appending or truncating then insert

    Output:
    * Staging data in AWS Redshift is inserted from staging tables to Fact table.
    """
    ui_color = '#F98866'
    
    sql_template = """
        INSERT INTO {}
        {};
        """

    @apply_defaults
    def __init__(self,
                 # Define  operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 query="",
                 insert="append",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query
        self.insert = insert

    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Loading fact table in Redshift")
        formatted_sql = LoadFactOperator.insert_sql.format(
            self.table,
            self.query,
            self.insert
        )
        redshift.run(formatted_sql)
