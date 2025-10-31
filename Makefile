.PHONY: reset-airflow init-airflow create-schema up-airflow down-airflow start ver-db

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

create-schema:
	@echo "[*] Creando tablas en la base de datos..."
	@if [ ! -f configs/schema.sql ]; then \
		echo "[ERROR] No se encontró el archivo configs/schema.sql"; \
		exit 1; \
	fi
	@echo "[*] Esperando a que PostgreSQL esté listo..."
	@sleep 5
	@cat configs/schema.sql | docker-compose exec -T postgres psql -U airflow -d airflow
	@echo "[OK] Tablas creadas correctamente"

up-airflow:
	docker-compose up -d

ver-db:
	docker-compose exec webserver python /opt/airflow/scripts/ver_db.py

start: reset-airflow init-airflow create-schema up-airflow
	@echo ""
	@echo "Airflow está listo!"
	@echo "   Accede a: http://localhost:8080"
	@echo "   Usuario: admin | Contraseña: admin"
	@echo ""