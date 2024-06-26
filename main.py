from psycopg2 import sql
import json
from validation_files.validateMinMaxFile import validateMinMaxFile
from connections import get_connection
import pandas as pd
import constants
import commonFunctions


def validateUploadedFile(data, job_log_id):    
    try:   
        
        # Process the job
        commonFunctions.processing(job_log_id)
        
        if data['module'].iloc[0] == constants.modules['MIN_MAX']:
            validateMinMaxFile(data, job_log_id)
                      
        
    except Exception:
        raise Exception("An error occurred in validateUploadedFile function.")


# Example usage
if __name__ == "__main__":

    json_data   = ''' {
        "file_id": 498
    } '''

    data = json.loads(json_data)
    print(data)
    
    query = sql.SQL("SELECT * FROM file_import_activities WHERE id = %s")
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (data['file_id'],))
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]  # Get column names
            if rows:
                # Convert the result into a DataFrame
                df = pd.DataFrame(rows, columns=columns)
            else:
                df = pd.DataFrame(columns=columns)
        
            if df.empty:
                raise Exception("File object not found for b2c store template import") 
    
    
    # Initiate a new process
    job_log_id = commonFunctions.initiate_process("FILE_IMPORT", df['module'].iloc[0], f"{df['module'].iloc[0]}_JOB", df['id'].iloc[0], df['created_by'].iloc[0])
    
    validateUploadedFile(df, job_log_id)
    

    # # Complete the job
    # commonFunctions.completed(job_log_id, error_log={"error": "Some error occurred"}, error_message="An error occurred during processing")
