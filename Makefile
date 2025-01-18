include .env

help:
	@echo "## docker-build		- Build Docker Images (arm64) including its inter-container network."
	@echo "## postgres			- Run a Postgres container"
	@echo "## spark			    - Run a Spark cluster, rebuild the postgres container, then create the destination tables."
	@echo "## jupyter			- Spinup jupyter notebook for testing and validation purposes."
	@echo "## airflow			- Spinup airflow scheduler and webserver."

docker-build:
	@echo '__________________________________________________________'
	@echo 'Building Docker Images ...'
	@echo '__________________________________________________________'
	@chmod 777 scripts/entrypoint.sh
	@docker network inspect batch-processing-network >/dev/null 2>&1 || docker network create batch-processing-network
	@echo '__________________________________________________________'
	@docker build -t batch-processing/spark -f ./docker/Dockerfile.spark .
	@echo '__________________________________________________________'
	@docker build -t batch-processing/airflow -f ./docker/Dockerfile.airflow .
	@echo '__________________________________________________________'
	@docker build -t batch-processing/jupyter -f ./docker/Dockerfile.jupyter .
	@echo '==========================================================='

jupyter:
	@echo '__________________________________________________________'
	@echo 'Creating Jupyter Notebook Cluster at http://localhost:${JUPYTER_PORT} ...'
	@echo '__________________________________________________________'
	@if ! docker images | grep -q "batch-processing/jupyter"; then \
		echo "Image not found locally, building..."; \
		docker build -t batch-processing/jupyter -f ./docker/Dockerfile.jupyter .; \
	fi
	@docker compose -f ./docker/docker-compose-jupyter.yml --env-file .env up -d
	@echo 'Created...'
	@echo 'Processing token...'
	@sleep 20
	@docker logs ${JUPYTER_CONTAINER_NAME} 2>&1 | grep '\?token\=' -m 1 | cut -d '=' -f2
	@echo '==========================================================='

spark:
	@echo '__________________________________________________________'
	@echo 'Creating Spark Cluster ...'
	@echo '__________________________________________________________'
	@docker compose -f ./docker/docker-compose-spark.yml --env-file .env up -d
	@echo '==========================================================='

airflow:
	@echo '__________________________________________________________'
	@echo 'Creating Airflow Instance ...'
	@echo '__________________________________________________________'
	@docker compose -f ./docker/docker-compose-airflow.yml --env-file .env up
	@echo '==========================================================='

postgres: postgres-create postgres-create-warehouse postgres-create-table postgres-ingest-csv

postgres-create:
	@docker compose -f ./docker/docker-compose-postgres.yml --env-file .env up -d
	@echo '__________________________________________________________'
	@echo 'Postgres container created at port ${POSTGRES_PORT}...'
	@echo '__________________________________________________________'
	@echo 'Postgres Docker Host	: ${POSTGRES_CONTAINER_NAME}' &&\
		echo 'Postgres Account	: ${POSTGRES_USER}' &&\
		echo 'Postgres password	: ${POSTGRES_PASSWORD}' &&\
		echo 'Postgres Db		: ${POSTGRES_DW_DB}'
	@sleep 5
	@echo '==========================================================='

postgres-create-table:
	@echo '__________________________________________________________'
	@echo 'Creating tables...'
	@echo '_________________________________________'
	@docker exec -it ${POSTGRES_CONTAINER_NAME} psql -U ${POSTGRES_USER} -d ${POSTGRES_DW_DB} -f sql/ddl-retail.sql
	@echo '==========================================================='

postgres-ingest-csv:
	@echo '__________________________________________________________'
	@echo 'Ingesting CSV...'
	@echo '_________________________________________'
	@docker exec -it ${POSTGRES_CONTAINER_NAME} psql -U ${POSTGRES_USER} -d ${POSTGRES_DW_DB} -f sql/ingest-retail.sql
	@echo '==========================================================='

postgres-create-warehouse:
	@echo '__________________________________________________________'
	@echo 'Creating Warehouse DB...'
	@echo '_________________________________________'
	@docker exec -it ${POSTGRES_CONTAINER_NAME} psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} -f sql/warehouse-retail.sql
	@echo '==========================================================='