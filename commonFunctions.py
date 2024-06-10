from io import StringIO
import boto3
import os
from dotenv import load_dotenv
import pandas as pd
import constants
import sys
from connections import get_connection
from psycopg2 import sql
import json
from datetime import datetime

load_dotenv()

conn = get_connection()
cur = conn.cursor()

s3_client = boto3.client('s3', aws_access_key_id=os.getenv('AWS_ACCESS_KEY'), aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))


def get_csv_from_s3(raw_file_path):  
    response = s3_client.get_object(Bucket=os.getenv('AWS_BUCKET'), Key=raw_file_path)
    row_data = response['Body'].read().decode('utf-8')

    chunks = pd.read_csv(StringIO(row_data), chunksize=constants.chunk_size, dtype=str)
    return chunks

def upload_csv_to_s3(dataframe, s3_file_path):
    try:
        # Convert the DataFrame to CSV
        csv_buffer = StringIO()
        dataframe.to_csv(csv_buffer, index=False)

        # Upload the CSV file to S3
        s3_client.put_object(Bucket=os.getenv('AWS_BUCKET'), Key=s3_file_path, Body=csv_buffer.getvalue(), ACL='public-read')
        
    except Exception as e:
        # Get the exception information
        exc_type, exc_value, exc_traceback = sys.exc_info()
        filename = exc_traceback.tb_frame.f_code.co_filename
        line_number = exc_traceback.tb_lineno
        
        # Print the error message from the last frame in the traceback
        failed_reason = f"Error occurred in {filename}, line {line_number} ==> {e}"
        raise Exception(failed_reason)


def get_product_data():
    query = "SELECT * FROM products WHERE deleted_at is NULL"
    return pd.read_sql(query, conn)

def get_store_data():
    query = "SELECT * FROM stores WHERE deleted_at is NULL"
    return pd.read_sql(query, conn)

def get_min_max_data():
    query = ''' SELECT * FROM min_max
                WHERE deleted_at is NULL '''
    return pd.read_sql(query, conn)


def initiate_process(module_name, sub_module_name, job_name, voucher_id, initiated_by):
    query = sql.SQL("""
        INSERT INTO job_status (module_name, sub_module_name, job_name, status, init_time, initiated_by, voucher_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING id
    """)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (module_name, sub_module_name, job_name, 
                                constants.FileImportStatus['INIT'], datetime.now(), 
                                initiated_by, voucher_id))
            log_id = cur.fetchone()[0]
    return log_id


def processing(log_id):
    query = sql.SQL("""
        UPDATE job_status
        SET start_time = %s, status = %s
        WHERE id = %s
    """)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (datetime.now(), 
                                constants.FileImportStatus['PROCESSING'], log_id))
            conn.commit()


def completed(log_id, error_log=None, error_message=None):
    query = sql.SQL("""
        UPDATE job_status
        SET error_log = %s, error_message = %s, end_time = %s, status = %s
        WHERE id = %s
    """)
    status = constants.FileImportStatus['FAILED'] if error_log or error_message else constants.FileImportStatus['PROCESSED']
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (json.dumps(error_log) if error_log else None, error_message, 
                                datetime.now(), status, log_id))
            conn.commit()

