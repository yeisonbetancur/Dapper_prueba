.PHONY: reset-airflow init-airflow up-airflow down-airflow start ver-db

down-airflow:
	docker-compose down --volumes

reset-airflow: down-airflow
	sudo chown -R $$(id -u):$$(id -g) logs dags plugins || true
	rm -rf logs/* dags/* plugins/*
	mkdir -p logs dags plugins
	chmod 777 logs dags plugins

init-airflow:
	docker-compose run --rm webserver airflow db init
	docker-compose run --rm webserver \
	  airflow users create \
	    --username admin --password admin \
	    --firstname Admin --lastname User \
	    --role Admin --email admin@example.com

up-airflow:
	docker-compose up -d


ver-db:
	docker-compose exec webserver python /opt/airflow/scripts/ver_db.py

start: reset-airflow init-airflow up-airflow
