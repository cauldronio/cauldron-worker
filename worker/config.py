from os import environ

DB_HOST = environ.get('DB_HOST')
DB_USER = environ.get('DB_USER')
DB_PASSWORD = environ.get('DB_PASSWORD')
DB_NAME = environ.get('DB_NAME')
WORKER_NAME = environ.get('WORKER_NAME')
DB_PORT = 3306
