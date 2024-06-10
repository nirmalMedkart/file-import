from connections import get_connection
from psycopg2 import sql
import pandas as pd
import boto3
from io import StringIO
import constants
import sys
from datetime import datetime
import os
from dotenv import load_dotenv
import jobStatus

load_dotenv()

# Database
conn = get_connection()
cur = conn.cursor()

file_import_update_query = sql.SQL("""
        UPDATE file_import_activities
        SET 
            status = %s,
            is_validated = %s,
            is_failed = %s,
            validated_path = %s,
            final_path = %s,
            records_created = %s,
            records_updated = %s,
            records_errored = %s,
            records_deleted = %s,
            failed_reason = %s,
            updated_at = %s,
            completed_at = %s
        WHERE id = %s
        """)


s3_client = boto3.client('s3', aws_access_key_id=os.getenv('AWS_ACCESS_KEY'), aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))


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


products_data = get_product_data()
stores_data = get_store_data() 
min_max_data = get_min_max_data()

# pd.set_option('display.max_rows', None)
# pd.set_option('display.max_columns', None)
# pd.set_option('display.width', None)
# pd.set_option('display.max_colwidth', None)
records_created = 0
records_errored = 0
records_updated = 0
records_deleted = 0


def validateMinMaxFile(data, job_log_id):
    global records_created
    global records_errored
    global records_updated
    global records_deleted
    
    try:
        response = s3_client.get_object(Bucket=os.getenv('AWS_BUCKET'), Key=data['raw_file_path'].iloc[0])
        row_data = response['Body'].read().decode('utf-8')

        chunks = pd.read_csv(StringIO(row_data), chunksize=constants.chunk_size, dtype=str)
        
        processed_chunks =[]
        comb_key_array = []
        for chunk in chunks:
            chunk['ws_code'] = pd.to_numeric(chunk['ws_code'], errors='coerce').astype('Int64')
            chunk['min_value'] = pd.to_numeric(chunk['min_value'], errors='coerce').astype('Int64')
            chunk['max_value'] = pd.to_numeric(chunk['max_value'], errors='coerce').astype('Int64')
            
            chunk['status'] = None
            chunk['message'] = None
          
            for index,row in chunk.iterrows():
                message = ""
                
                if row['ws_code'] is None or pd.isna(row['ws_code']):
                    message += "ws_code not found for row | "
                    
                if row['store_code'] is None or pd.isna(row['store_code']):
                    message += "store_code not found for row | "
                    
                if row['min_value'] is None or pd.isna(row['min_value']):
                    message += "min_value not found for row | "
                
                if row['max_value'] is None or pd.isna(row['max_value']):
                    message += "max_value not found for row | "
                
                if pd.notna(row['ws_code']):    
                    if row['ws_code'] not in products_data['ws_code'].tolist():
                        message += "Invalid Wondersoft code for row | "
                        
                    if not products_data[products_data['ws_code'] == row['ws_code']]['is_active'].iloc[0]:
                        message += "Product is inactive | "
                        
                    if products_data[products_data['ws_code'] == row['ws_code']]['is_discontinued'].iloc[0]:
                        message += "Product is discontinued | "
                    
                if pd.notna(row['store_code']):
                    if row['store_code'] not in stores_data['ws_alternate_code'].tolist():
                        message += "Invalid store code for row | "
                        
                    if not stores_data[stores_data['ws_alternate_code'] == row['store_code']]['is_active'].iloc[0]:
                        message += "Store is inactive | "
                        
                if data['operation'].iloc[0] != constants.operations['DELETE']:
                    if row['min_value'] is None or pd.isna(row['min_value']):
                        message += "min_value not found for row | "
                    
                    if row['max_value'] is None or pd.isna(row['max_value']):
                        message += "max_value not found for row | "
                            
                    if pd.notna(row['min_value']):
                        if not isinstance(row['min_value'], int):
                            message += "Min value can be integer only | "
                            
                        if row['min_value'] < 0:
                            message += "Min value cannot be negative | "
                            
                    if pd.notna(row['max_value']):   
                        if not isinstance(row['max_value'], int):
                            message += "Max value can be integer only | "
                    if pd.notna(row['max_value']) and pd.notna(row['min_value']):      
                        if row['max_value'] < row['min_value']:
                            message += "Max value cannot be less than min value | "

                if pd.notna(row['ws_code']) and pd.notna(row['store_code']):
                    current_combination_key = f"{row['ws_code']}_{row['store_code']}"
                    if current_combination_key in comb_key_array:
                        message += "Product is repeated in file | "
                    comb_key_array.append(current_combination_key)

                    if row['ws_code'] in products_data['ws_code'].tolist():
                        productId = products_data[products_data['ws_code'] == row['ws_code']]['id'].iloc[0]
                    if row['store_code'] in stores_data['ws_alternate_code'].tolist():
                        storeId = stores_data[stores_data['ws_alternate_code'] == row['store_code']]['id'].iloc[0]
                    
                    if (data['operation'].iloc[0] == constants.operations['UPDATE']) and (data['operation'].iloc[0] == constants.operations['DELETE']):
                        if productId not in min_max_data['product_id'].tolist() and storeId not in min_max_data['store_id'].tolist():
                            message += 'Provided wondersoft code and store code not found in DB | '
                            
                    elif data['operation'].iloc[0] == constants.operations['ADD']:
                        if productId in min_max_data['product_id'].tolist() and storeId in min_max_data['store_id'].tolist():
                            message += 'Provided wonder soft and store code is already available in DB | '
                        
                if message:
                    chunk.loc[index, 'message'] = message
                    chunk.loc[index, 'status'] = False
                    records_errored += 1
                else:
                    chunk.loc[index, 'status'] = True
                    records_created += 1
                       
            processed_chunks.append(chunk)
        validated_data = pd.concat(processed_chunks, ignore_index=True)
       
        file_location = f"FileImport/validatedFiles/{data['module'].iloc[0]}/{data['id'].iloc[0]}minMaxValidated.csv"
        
        upload_csv_to_s3(validated_data, file_location)
        
        cur.execute(file_import_update_query, (
            constants.FileImportStatus['VALIDATED'], #status 
            True, # is_validated
            False, #  is_failed
            file_location, # validated_path
            None, #  final_path 
            records_created, # records_created
            records_updated, # records_updated
            records_errored, # records_errored
            records_deleted, # records_deleted
            None, # failed_reason
            datetime.now(), # updated_at
            None, # completed_at
            int(data['id'].iloc[0])
        ))
        conn.commit()
        
        store_data(validated_data, data, job_log_id)
        
        # Complete the job
        jobStatus.completed(job_log_id, error_log=None, error_message=None)
        
    except Exception as e:
        print(f'===>>{e}')
        # Get the exception information
        exc_type, exc_value, exc_traceback = sys.exc_info()
        filename = exc_traceback.tb_frame.f_code.co_filename
        line_number = exc_traceback.tb_lineno

        # Print the error message from the last frame in the traceback
        failed_reason = f"Error occurred in {filename}, line {line_number} ==> {e}"
        print(failed_reason)
        cur.execute(file_import_update_query, (
            constants.FileImportStatus['FAILED'], #status 
            False, # is_validated
            True, #  is_failed
            None, # validated_path
            None, #  final_path 
            0, # records_created
            0, # records_updated
            0, # records_errored
            0, # records_deleted
            failed_reason, # failed_reason
            datetime.now(), # updated_at
            None, # completed_at
            int(data['id'].iloc[0])
        ))
        conn.commit()
        
        # Complete the job
        jobStatus.completed(job_log_id, error_log={"error": "Some error occurred while validating file"}, error_message=failed_reason)
        
def store_data(validated_data, file_data, job_log_id):
    try:
        cur.execute(file_import_update_query, (
                constants.FileImportStatus['PROCESSING'], #status 
                True, # is_validated
                False, #  is_failed
                None, # validated_path
                None, #  final_path 
                records_created, # records_created
                records_updated, # records_updated
                records_errored, # records_errored
                records_deleted, # records_deleted
                None, # failed_reason
                datetime.now(), # updated_at
                None, # completed_at
                int(file_data['id'].iloc[0])
            ))
        
        
        if not validated_data.empty:
            sql_values = []
            for index,row in validated_data.iterrows():
                if row['status']:
                    if file_data['operation'].iloc[0] == constants.operations['ADD']:
                        sql_values.append((
                            int(stores_data[stores_data['ws_alternate_code'] == row['store_code']]['id'].iloc[0]),
                            int(products_data[products_data['ws_code'] == row['ws_code']]['id'].iloc[0]),
                            row['min_value'],
                            row['max_value'],
                            int(file_data['created_by'].loc[0]),
                            f'{datetime.now()}'
                        ))
                        
                    if file_data['operation'].iloc[0] == constants.operations['UPDATE']:
                        sql_values.append(f"""
                                            UPDATE min_max
                                            SET min_value = {int(row['min_value'])},
                                            max_value = {int(row['max_value'])},
                                            updated_at = '{datetime.now()}',
                                            updated_by = {int(file_data['created_by'].iloc[0])}
                                            WHERE store_id = '{int(stores_data[stores_data['ws_alternate_code'] == row['store_code']]['id'].iloc[0])}'
                                            AND product_id =  {int(products_data[products_data['ws_code'] == row['ws_code']]['id'].iloc[0])}
                                        """)
                    
                    if file_data['operation'].iloc[0] == constants.operations['DELETE']:
                        sql_values.append(f"""
                                            UPDATE min_max
                                            SET deleted_at = '{datetime.now()}',
                                            deleted_by = {int(file_data['created_by'].iloc[0])}
                                            WHERE store_id = '{int(stores_data[stores_data['ws_alternate_code'] == row['store_code']]['id'].iloc[0])}'
                                            AND product_id =  {int(products_data[products_data['ws_code'] == row['ws_code']]['id'].iloc[0])}
                                        """)
        
            if sql_values and file_data['operation'].iloc[0] == constants.operations['ADD']:
                insert_query = """
                                INSERT INTO min_max (store_id, product_id, min_value, max_value, created_by, created_at)
                                VALUES (%s, %s, %s, %s, %s, %s)
                            """
                cur.executemany(insert_query, sql_values)
                conn.commit()
                    
            if sql_values and file_data['operation'].iloc[0] == constants.operations['UPDATE']:
                update_query = "; ".join(sql_values)
                cur.execute(update_query)
                conn.commit()
                
            if sql_values and file_data['operation'].iloc[0] == constants.operations['DELETE']:
                delete_query = "; ".join(sql_values)
                cur.execute(delete_query)
                conn.commit()

            final_path = f"FileImport/validatedFiles/{file_data['module'].iloc[0]}/{file_data['id'].iloc[0]}minMaxProcessed.csv"
            upload_csv_to_s3(validated_data, final_path)
            
            
        cur.execute(file_import_update_query, (
            constants.FileImportStatus['PROCESSED'], #status 
            True, # is_validated
            False, #  is_failed
            None, # validated_path
            final_path, #  final_path 
            records_created, # records_created
            records_updated, # records_updated
            records_errored, # records_errored
            records_deleted, # records_deleted
            None, # failed_reason
            datetime.now(), # updated_at
            datetime.now(), # completed_at
            int(file_data['id'].iloc[0])
        ))
        conn.commit()
            
    except Exception as e:
        conn.rollback()
        # Get the exception information
        exc_type, exc_value, exc_traceback = sys.exc_info()
        filename = exc_traceback.tb_frame.f_code.co_filename
        line_number = exc_traceback.tb_lineno

        # Print the error message from the last frame in the traceback
        failed_reason = f"Error occurred in {filename}, line {line_number} ==> {e}"
        
        cur.execute(file_import_update_query, (
            constants.FileImportStatus['FAILED'], #status 
            False, # is_validated
            True, #  is_failed
            None, # validated_path
            None, #  final_path 
            0, # records_created
            0, # records_updated
            0, # records_errored
            0, # records_deleted
            failed_reason, # failed_reason
            datetime.now(), # updated_at
            None, # completed_at
            int(file_data['id'].iloc[0])
        ))
        conn.commit()   
        # Complete the job
        jobStatus.completed(job_log_id, error_log={"error": "Some error occurred while validating file"}, error_message=failed_reason)
        print(failed_reason)
        sys.exit()
    
    finally:
        cur.close()
        conn.close()
            


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
