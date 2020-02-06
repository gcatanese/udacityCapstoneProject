# udacityCapstoneProject
Data Engineering Nanodegree Capstone Project

# Goals

The goals of the final Capstone Project is to analyse large datasets and identify a strategy to 
load the data into a Data Warehouse where it can be analysed and queried.

## Datasets

The datasets available within the Udacity Project workspace have been used:

- **World Temperature Data**: source Kaggle
- **U.S. City Demographic Data**: source OpenSoft
- **Airport Code Table**: source Datahub
  

# Uses Cases

The datasets can be used by data scientists and analysts to gain deeper understanding of the raising climate temperatures 
(within continents, countries, cities and around airports.). The findings can be also projected on US demographics to
explore how different genders or ethnic groups might be impacted.

# Tools

### Airflow 
Apache Airflow is an open-source tool for designing and executing computational workflows and data processing pipelines.
It was adopted in this project for providing a visual representations on the workflow and create re-usable operators. 

 
### Redshift and S3

Amazon Redshift is an cloud-based data warehouse solution which supports storing and querying petabytes of structured 
and semi-structured data using standard SQL. It tightly integrates with Amazon S3 allowing to fetch large amount of
data as well as saving the output of the data computation.


# Data Model

The data model involves 3 staging tables (staging_temperature_data, staging_us_city_data, staging_airport_code_data) 
which map the structure of the corresponding dataset.
The other tables represent the Star Scheme with a Fact table (temperatures) and 3 dimensions (Cities, Airports, Time).

# Solution

There are three main steps involved in the solution:
* Pre-Process: one-off preprocessing of the data: this is performed using Python and Panda library and aims at preparing the 
 data for the ETL 
* Create Data Model: create the data model which will store the data
* Perform ETL: execute the ETL pipeline


## Pre-Process

The data sets have been explored with Jupyter Notebook to assess size, possible quality issues, suitable
primary keys: the rows with incomplete data are removed and duplicated records as ignored.

The data clean up tasks performed are:
* load dataset in Pandas dataframe
* display size (count) and overview (head)
* identify primary keys (columns with unique values)
* drop rows where essential attribute were not provided (i.e. AverageTemperature, City, Country)
* rename columns 
* convert to JSON format 

Once processed the data is store on S3 ***s3://beppe-udacity-capstone/capstone***

## Create Data Model

The creation of the data model is defined with its own DAG 

![Alt text](wiki/data_model_dag.jpg?raw=true "Title")

## Perform ETL

The ETL is modeled with the etl.dag

![Alt text](wiki/etl_dag.jpg?raw=true "Title")



# Executing the pipeline

Start a RedShift cluster (note down attributes as they are required during the Airflow setup)

Start Airflow and configure the following:
 
***AWS Credentials***  
In Airflow Administration create an 'Amazon Web Service' connection, see the following as an example

![Alt text](wiki/aws_services.png?raw=true "Title")

The AWS credentials must be used.

***Postgres Connection***    
In Airflow Administration create a 'Postgres' connection, see the following as an example:

![Alt text](wiki/redshift_connection.png?raw=true "Title")

The Redshift host must be entered.

***Run the DAGs***  

In Airflow first execute the 'create_data_model_dag'

Proceed and execute the 'etl.tag'

# What If

***If the data was increased by 100x*** the whole process needs to scale up.  
The initial pre-process could be done using Apache Spark which, due to large volume distributed processing capabilities,
can handle a lot more data.   
Redhshift cluster could add additional (more powerful) nodes while data engineers would leverage the Redshift features
(distkey, sortkey) to maximise throughput of the sql joins and sorting.

***If the pipelines were run on a daily basis by 7am*** the configuration of the ETL DAB would be modified to run 
daily, together with the data files which would include a timestamp in the filename (in order to process the data 
relevant to the day)

***If the database needed to be accessed by 100+ people*** the data warehouse would rely on Redshift scalability
capabilities and possibly look at the Concurrency Scaling feature (outside the scope of this course).

