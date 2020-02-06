""" Create data model """

from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import PostgresOperator

from operators.data_quality import DataQualityOperator

default_args = {
    'owner': 'beppe',
    'schedule_interval': None,
    'start_date': datetime(2020, 1, 1),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 0
}

dag = DAG('create_data_model_dag',
          catchup=False,
          default_args=default_args,
          description='Drop and reCreate tables'
          )

start_operator = DummyOperator(task_id='begin', dag=dag)

drop_tables = PostgresOperator(
    task_id="drop_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql="drop_tables.sql"
)

create_tables = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql="create_tables.sql"
)

end_operator = DummyOperator(task_id='end', dag=dag)

start_operator >> drop_tables
drop_tables >> create_tables
create_tables >> end_operator
