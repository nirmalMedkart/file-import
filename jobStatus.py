from connections import get_connection
from psycopg2 import sql
from datetime import datetime
import constants
import json

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

