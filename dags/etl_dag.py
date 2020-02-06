""" DAG of the Capstone Project ETL"""

from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from operators.data_quality import DataQualityOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.stage_csv_in_redshift import StageCsvToRedshiftOperator
from operators.stage_json_in_redshift import StageJsonToRedshiftOperator

from helpers import SqlQueries

default_args = {
    'owner': 'beppe',
    'schedule_interval': None,
    'start_date': datetime(2020, 1, 1),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 0
}

dag = DAG('etl_dag',
          catchup=False,
          default_args=default_args,
          description='Perform ETL pipeline'
          )

start_operator = DummyOperator(task_id='begin', dag=dag)


cities_staging = StageCsvToRedshiftOperator(
    task_id = 'cities_staging',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_us_city_data',
    s3_bucket="beppe-udacity-capstone",
    s3_key="capstone/us-cities-demographics.csv",
    delimiter=";"
)

temperatures_staging = StageJsonToRedshiftOperator(
    task_id = 'temperatures_staging',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_temperature_data',
    s3_bucket="beppe-udacity-capstone",
    s3_key="capstone/weather_data_"
)

airport_codes_staging = StageJsonToRedshiftOperator(
    task_id = 'airport_codes_staging',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_airport_code_data',
    s3_bucket="beppe-udacity-capstone",
    s3_key="capstone/airport_code_"
)

end_staging = DummyOperator(task_id='end_staging', dag=dag)


# Load Fact table
load_fact_table = LoadFactOperator(
    task_id='load_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table_name="fact_temperature",
    sql_insert_stmt=SqlQueries.fact_table_insert,
    truncate=False
)

# Load Dimension table
load_time_dimension_table = LoadDimensionOperator(
    task_id='load_time_dimension_table',
    dag=dag,
    redshift_conn_id="redshift",
    table_name="time",
    sql_insert_stmt=SqlQueries.time_table_insert,
    truncate=False
)

# Load Dimension table
load_airport_dimension_table = LoadDimensionOperator(
    task_id='load_airport_dimension_table',
    dag=dag,
    redshift_conn_id="redshift",
    table_name="airport",
    sql_insert_stmt=SqlQueries.airport_table_insert,
    truncate=False
)

# Load Dimension table
load_demographic_dimension_table = LoadDimensionOperator(
    task_id='load_demographic_dimension_table',
    dag=dag,
    redshift_conn_id="redshift",
    table_name="demographic",
    sql_insert_stmt=SqlQueries.demographic_table_insert,
    truncate=False
)

end_loading = DummyOperator(task_id='end_loading',  dag=dag)

run_quality_checks_on_fact_table = DataQualityOperator(
    task_id='run_quality_checks_on_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="fact_temperature"
)

run_quality_checks_on_time_table = DataQualityOperator(
    task_id='run_quality_checks_on_time_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time"
)

run_quality_checks_on_airport_table = DataQualityOperator(
    task_id='run_quality_checks_on_airport_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="airport"
)

run_quality_checks_on_demographic_table = DataQualityOperator(
    task_id='run_quality_checks_on_demographic_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="demographic"
)

end_operator = DummyOperator(task_id='end',  dag=dag)


start_operator >> temperatures_staging
start_operator >> cities_staging
start_operator >> airport_codes_staging

temperatures_staging >> end_staging
cities_staging >> end_staging
airport_codes_staging >> end_staging

end_staging >> load_fact_table

load_fact_table >> load_airport_dimension_table
load_fact_table >> load_demographic_dimension_table
load_fact_table >> load_time_dimension_table

load_airport_dimension_table >> end_loading
load_demographic_dimension_table >> end_loading
load_time_dimension_table >> end_loading

end_loading >> run_quality_checks_on_fact_table
end_loading >> run_quality_checks_on_airport_table
end_loading >> run_quality_checks_on_demographic_table
end_loading >> run_quality_checks_on_time_table

run_quality_checks_on_fact_table >> end_operator
run_quality_checks_on_time_table >> end_operator
run_quality_checks_on_demographic_table >> end_operator
run_quality_checks_on_airport_table >> end_operator

