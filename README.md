# airflow-nanodegree

## Background
The primary goal was to create an Airflow DAG that manages a fictional music streaming company's ETL process. This required the construction of four custom Operator classes: `StageToRedshiftOperator`, `LoadFactOperator`, `LoadDimensionOperator`, and a `DataQualityOperator`, to help break the pipeline up into reusable components. All of these operators and task instances run SQL statements against a Redshift database. 
 
## Operators

### Stage Operator
The stage operator loads JSON formatted files from S3 to Amazon Redshift, creating and running a COPY statement based on the parameters provided. 

### Fact and Dimension Operators
The facts and dimension operators execute SQL that loads data from the staging tables into the finalized star schema. Although a postgres operator could be sufficient, custom operators allow us to use the truncate-insert pattern for dimension tables, where they are emptied before loading, versus append-only inserts for the fact table. 
 
### Data Quality Operator
The data quality operator is used to run checks on the final data. The operator allows the DAG owner to call one of the following data checks on any different table: `has_rows`, `row_count_between`, `no_nulls`, and `all_distinct`. If any of the specified data quality checks are not met, Airflow will raise an exception.   
