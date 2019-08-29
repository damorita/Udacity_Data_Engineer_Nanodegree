from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """Apache Airflow Operator to load data
        from AWS Redshift staging tables to Dimension tables.
    Keyword arguments:
    * redshift_conn_id      -- AWS Redshift connection ID
    * table                 -- AWS Redshift target table name
    * query                 -- Query name to be used from SqlQueries.
    * insert                -- How to insert data to table
                                (append: on top of existing data 
                                 truncate_insert: truncate table + insert new data)

    Output:
    * Staging data in AWS Redshift is inserted from staging tables to Dimension table.
    """

    ui_color = '#80BD9E'
    
    insert_sql = """
        TRUNCATE TABLE {};
        INSERT INTO {}
        {};
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 query="",
                 insert="append",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query
        self.insert = insert

    def execute(self, context):
        #self.log.info('LoadDimensionOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Loading dimension table {self.table} in Redshift")
        formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.table,
            self.query,
            self.insert
        )
        redshift.run(formatted_sql)
