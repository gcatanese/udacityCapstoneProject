from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Loads the Fact table

    :param redshift_conn_id: Redshift connection ID
    :param table_name: Table name
    :param sql_insert_stmt: SQL statement which performs the loading of the data
    :param truncate: when True truncate the table
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_name="",
                 sql_insert_stmt="",
                 truncate=False,
                 *args, **kwargs):
        """
        Initialise the operator

        :param redshift_conn_id: Redshift connection ID
        :param table_name: Table name
        :param sql_insert_stmt: SQL statement which performs the loading of the data
        :param truncate: when True truncate the table

        """

        super(LoadFactOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.sql_insert_stmt = sql_insert_stmt
        self.truncate = truncate

    def execute(self, context):
        """
        Executes the operator logic

        :param context:
        """

        self.log.info('Loading Fact table ' + self.table_name)

        redshift_hook = PostgresHook(self.redshift_conn_id)

        if self.truncate:
            sql_stmt = "TRUNCATE {} "
            formatted_sql_stmt = sql_stmt.format(self.table_name)
            self.log.info(formatted_sql_stmt)
            redshift_hook.run(formatted_sql_stmt)

        sql_stmt = "{}"
        formatted_sql_stmt = sql_stmt.format(self.sql_insert_stmt)
        self.log.info(formatted_sql_stmt)

        redshift_hook.run(formatted_sql_stmt)
