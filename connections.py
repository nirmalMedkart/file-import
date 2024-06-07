import psycopg2
from psycopg2 import sql

DB_CONNECTION='pgsql'
DB_HOST='mk-pos-dev.cbfy2umpswje.ap-south-1.rds.amazonaws.com'
DB_PORT=5432
DB_DATABASE='pos-dev'
DB_USERNAME='postgres'
DB_PASSWORD='stivEthAkETYLEs'

# Database configuration
DATABASE_CONFIG = {
    'dbname': DB_DATABASE,
    'user': DB_USERNAME,
    'password': DB_PASSWORD,
    'host': DB_HOST,
    'port': 5432
}

def get_connection():
    return psycopg2.connect(**DATABASE_CONFIG)

