import psycopg2
import pandas as pd
import os
from urllib.parse import urlparse
from typing import Dict


class DatabaseManager:
    def __init__(self):
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            params = self._get_db_env()
            
            self.connection = psycopg2.connect(
                dbname=params['dbname'],
                user=params['user'],
                password=params['password'],
                host=params['host'],
                port=params['port'],
            )
            self.cursor = self.connection.cursor()
            return True
        except Exception as e:
            print(f"Database connection error: {e}")
            return False
        
    def _get_db_env(self) -> Dict[str, str]:
        uri = os.environ.get('AIRFLOW__CORE__SQL_ALCHEMY_CONN')
        if uri:
            # Forma Esperada: postgresql+psycopg2://user:pass@host:port/dbname
            parsed = urlparse(uri)
            dbname = parsed.path.lstrip('/') or 'airflow'
            user = parsed.username or 'airflow'
            password = parsed.password or 'airflow'
            host = parsed.hostname or 'postgres'
            port = parsed.port or 5432
            return {
                'dbname': dbname,
                'user': user,
                'password': password,
                'host': host,
                'port': int(port),
            }

        # Fallback a variables de entorno
        return {
            'dbname': os.environ.get('DB_NAME', 'airflow'),
            'user': os.environ.get('DB_USER', 'airflow'),
            'password': os.environ.get('DB_PASSWORD', 'airflow'),
            'host': os.environ.get('DB_HOST', 'postgres'),
            'port': int(os.environ.get('DB_PORT', '5432')),
        }

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def execute_query(self, query, params=None):
        if not self.cursor:
            raise Exception("Database not connected")
        self.cursor.execute(query, params)
        return self.cursor.fetchall()

    def bulk_insert(self, df, table_name):
        if not self.connection or not self.cursor:
            raise Exception("Database not connected")
        
        try:
            df = df.astype(object).where(pd.notnull(df), None)
            columns_for_sql = ", ".join([f'"{col}"' for col in df.columns])
            placeholders = ", ".join(["%s"] * len(df.columns))
            
            insert_query = f"INSERT INTO {table_name} ({columns_for_sql}) VALUES ({placeholders})"
            records_to_insert = [tuple(x) for x in df.values]
            
            self.cursor.executemany(insert_query, records_to_insert)
            self.connection.commit()
            return len(df)
        except Exception as e:
            self.connection.rollback()
            raise Exception(f"Error inserting into {table_name}: {str(e)}")
        

