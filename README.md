# Retail Data Analysis using PySpark

This project implements a batch data processing pipeline for analyzing retail data using PySpark. The pipeline performs tasks such as data extraction, transformation, and analysis, and stores the results back into a PostgreSQL database. The Airflow DAG orchestrates the process, and Docker is used for containerized deployment.

## Project Overview

### Key Features

#### 1. Data Extraction:
Reads retail data from a PostgreSQL database.

#### 2. Data Transformation:
Cleans and transforms the data, including handling nulls, type conversions, and adding new columns.

#### 3. Data Analysis:
- Aggregation: Revenue per country and top 10 products.
- Customer Insights: Churn prediction and RFM analysis.
- Sales Trends: Monthly revenue analysis.
- Results Storage: Writes analysis results back to the PostgreSQL database.

## Tools and Technologies

```
PySpark: For data processing and analysis.

PostgreSQL: As the data source and result storage.

Apache Airflow: For orchestrating the pipeline.

Docker: For containerized deployment.
```

### JDBC Details

The connection to PostgreSQL is established using JDBC. The JDBC URL format used in this project is as follows:
```
jdbc:postgresql://<POSTGRES_HOST>:<POSTGRES_PORT>/<POSTGRES_DATABASE>
```

The required JDBC properties include:

`user`: Username for the database.
`password`: Password for the database.
`driver`: org.postgresql.Driver
`stringtype`: unspecified (to handle text fields correctly).

## Repository Structure
```
dag/: Contains the Airflow DAG definition.

spark-scripts/: Contains the PySpark script for data processing and analysis.

Dockerfile: Configures the Docker image for the project.

docker-compose.yml: Defines the services for Docker containers.

Makefile: Automates setup and execution tasks.
```

## Requirements

Ensure the following are installed on your system:
1. Docker and Docker Compose

```
make utility
```

2. Setup and Execution

To run this project locally, follow these steps:

Clone the Repository
```
git clone <repository_url>
cd <repository_name>
```

3. Build the Docker Images

```
make docker-build
```

4. Start the PostgreSQL Database

```
make postgres
```

5. Start the Spark Container

```
make spark
```

6. Start the Airflow Services

```
make airflow
```

7. Access Airflow
Open your browser and navigate to `http://localhost:8091` to access the Airflow web interface. Use the default credentials (airflow/airflow) to log in.

8. Trigger the DAG
In the Airflow UI, trigger the `pyspark_retail_analysis` DAG to start the pipeline.

## Environment Variables

The following environment variables need to be set for the pipeline to run successfully:

```
POSTGRES_CONTAINER_NAME: Name of the PostgreSQL container.

POSTGRES_PORT: Port for the PostgreSQL database.

POSTGRES_DW_DB: Name of the database.

POSTGRES_USER: Username for the database.

POSTGRES_PASSWORD: Password for the database.
```

## Results

The analysis results will be written back to the following PostgreSQL tables:

```
revenue_per_country
top_products
last_purchase
churn_analysis
rfm_df
monthly_sales
```

## Troubleshooting

If you encounter issues, ensure that all services are running properly using docker ps. For more detailed logs, check the Airflow and Spark logs in their respective containers.

Let me know if you need further adjustments!