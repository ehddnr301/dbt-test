AIRFLOW_CONFIG := AIRFLOW_HOME=/opt/airflow

start: # Docker compose 를 통해 Airflow 실행
	$(AIRFLOW_CONFIG) docker compose -f docker-compose.yaml --profile flower up -d
	
stop: # Docker compose 를 통해 실행한 Airflow 종료
	$(AIRFLOW_CONFIG) docker compose down --volumes --remove-orphans