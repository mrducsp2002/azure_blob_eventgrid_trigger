import json
import logging
import os
import azure.functions as func
from src.database import get_mongo_db
from src.processor import process_blob_stream
from src.generator import generate_questions_logic, regenerate_questions_logic
from src.practice import handle_viva_message, start_viva_session
from azure.storage.blob import generate_blob_sas, BlobSasPermissions
from datetime import datetime, timedelta, timezone

app = func.FunctionApp()

# ==========================================
#  1A. HTTP API: Question Generation and Regeneration
# ==========================================
@app.route(route="generate_iviva_questions", auth_level=func.AuthLevel.FUNCTION)
def generate_iviva_question(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('HTTP Trigger: Processing IVIVA Question Generation.')

    try:
        req_body = req.get_json()
        student_id = req_body.get('student_id')
        unit_code = req_body.get('unit_code')
        session_year = req_body.get('session_year')
        assignment = req_body.get('assignment')
    except ValueError:
        return func.HttpResponse(
            "Invalid request body. Please provide JSON with student_id, unit_code, session_year, assignment.",
            status_code=400
        )
    if not all([student_id, unit_code, session_year, assignment]):
        return func.HttpResponse(
            "Missing required parameters. Please provide student_id, unit_code, session_year, assignment.",
            status_code=400
        )

    # Generate questions logic
    try:
        question_generated = generate_questions_logic(
            student_id, unit_code, session_year, assignment)
        return func.HttpResponse(
            json.dumps(question_generated),
            mimetype="application/json",
            status_code=200
        )

    except ValueError as ve:
        return func.HttpResponse(str(ve), status_code=404)

    except Exception as e:
        # Handle unexpected crashes
        logging.error(f"Internal Error: {e}")
        return func.HttpResponse(f"Error generating questions: {str(e)}", status_code=500)

@app.route(route="iviva_question_regeneration", auth_level=func.AuthLevel.FUNCTION)
def regenerate_iviva_question(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('HTTP Trigger: Processing IVIVA Question Regeneration.')

    try:
        req_body = req.get_json()
        current_question = req_body.get('current_question')
        user_comment = req_body.get('user_comment')

    except ValueError:
        return func.HttpResponse(
            "Invalid request body. Please provide JSON with current_question and user_comment.",
            status_code=400
        )
    if not all([current_question, user_comment]):
        return func.HttpResponse(
            "Missing required parameters. Please provide current_question and user_comment.",
            status_code=400
        )

    # Regenerate questions logic
    try:
        question_regenerated = regenerate_questions_logic(
            current_question, user_comment)
        return func.HttpResponse(
            json.dumps(question_regenerated),
            mimetype="application/json",
            status_code=200
        )

    except ValueError as ve:
        return func.HttpResponse(str(ve), status_code=404)

    except Exception as e:
        # Handle unexpected crashes
        logging.error(f"Internal Error: {e}")
        return func.HttpResponse(f"Error regenerating questions: {str(e)}", status_code=500)

# ==========================================
#  1B. HTTP API: Viva Chat Sessions
# ==========================================
@app.route(route="viva", auth_level=func.AuthLevel.FUNCTION)
def viva_start(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("HTTP Trigger: Starting Viva Session.")
    try:
        payload = req.get_json()
    except ValueError:
        return func.HttpResponse(
            "Invalid request body. Please provide JSON with student_id, unit_code, session_year, assignment.",
            status_code=400,
        )

    try:
        response_body = start_viva_session(payload)
        return func.HttpResponse(
            json.dumps(response_body),
            mimetype="application/json",
            status_code=200,
        )
    except ValueError as ve:
        return func.HttpResponse(str(ve), status_code=400)
    except Exception as e:
        logging.error(f"Internal Error: {e}")
        return func.HttpResponse(f"Error starting viva: {str(e)}", status_code=500)


@app.route(route="viva/message", auth_level=func.AuthLevel.FUNCTION)
def viva_message(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("HTTP Trigger: Viva Session Message.")
    try:
        payload = req.get_json()
    except ValueError:
        return func.HttpResponse(
            "Invalid request body. Please provide JSON with session_id and user_message.",
            status_code=400,
        )

    try:
        response_body = handle_viva_message(payload)
        return func.HttpResponse(
            json.dumps(response_body),
            mimetype="application/json",
            status_code=200,
        )
    except KeyError as ke:
        return func.HttpResponse(str(ke), status_code=404)
    except ValueError as ve:
        return func.HttpResponse(str(ve), status_code=400)
    except Exception as e:
        logging.error(f"Internal Error: {e}")
        return func.HttpResponse(f"Error processing message: {str(e)}", status_code=500)

        
# ==========================================
#  2. Blob Trigger: Document Uploads
# ==========================================

# --- SHARED LOGIC ---
def _handle_blob_event(myblob: func.InputStream, target_collection_name: str):
    """
    Processes the blob and saves it to the specific MongoDB collection.
    """
    logging.info(
        f"Processing blob: {myblob.name} -> Target Collection: {target_collection_name}")

    try:
        # 1. Get the Default Connection (Cached)
        db = get_mongo_db()
        target_collection = db[target_collection_name]

        # 3. Read Blob
        blob_content = myblob.read()

        # 4. Process
        process_blob_stream(blob_content, myblob.name, target_collection)

        logging.info(f"Successfully saved to {target_collection_name}")

    except Exception as e:
        logging.error(
            f"Error processing {myblob.name}: {str(e)}", exc_info=True)
        raise


@app.function_name(name="StudentAssignmentsUpload")
@app.blob_trigger(
    arg_name="myblob", 
    path="iviva-student-assignments/{name}", 
    source="EventGrid",
    connection="AzureWebJobsStorage")
def student_assignments_upload(myblob: func.InputStream):
    """
    Azure Function triggered by Blob storage events via Event Grid.
    Processes the blob and saves to 'assignments' collection.
    """
    _handle_blob_event(myblob, target_collection_name="iviva-student-assignments")
    
# Upload assessment brief
@app.function_name(name="BriefUpload")
@app.blob_trigger(arg_name="myblob", 
                  path="iviva-staff-assessment-brief/{name}", 
                  source="EventGrid",
                  connection="AzureWebJobsStorage")
def brief_upload(myblob: func.InputStream):
    _handle_blob_event(
        myblob, target_collection_name="iviva-staff-assessment-brief")
    
# Upload assessment rubrics
@app.function_name(name="RubricUpload")
@app.blob_trigger(arg_name="myblob", 
                  path="iviva-staff-assessment-rubrics/{name}", 
                  source="EventGrid",
                  connection="AzureWebJobsStorage")
def rubric_upload(myblob: func.InputStream):
    _handle_blob_event(
        myblob, target_collection_name="iviva-staff-assessment-rubrics")


# ==========================================
#  3. SAS token generation for secure uploads
# ==========================================
@app.route(route="generate_sas_token", auth_level=func.AuthLevel.FUNCTION)
def generate_sas_token(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('SAS Token generation requested')
    
    # Fetch data from request
    try: 
        req_body = req.get_json()
    except ValueError:
        return func.HttpResponse(
            "Invalid request body. Please provide JSON with containerName and blobName.",
            status_code=400
        )
        
    container_name = req_body.get('containerName')
    blob_name = req_body.get('blobName')
    
    if not container_name or not blob_name:
        return func.HttpResponse(
            "Missing required parameters. Please provide containerName and blobName.",
            status_code=400
        )
        
    # Only allow upload on specific container 
    if container_name not in [
        "iviva-student-assignments", 
        "iviva-staff-assessment-brief", 
        "iviva-staff-assessment-rubrics"
        ]:
        return func.HttpResponse(
            "Unallowed container.",
            status_code=403
        )
        
    # Get secrets and generate SAS token
    account_name = os.environ.get("STORAGE_ACCOUNT_NAME")
    account_key = os.environ.get("STORAGE_ACCOUNT_KEY")
    
    if not all([account_name, account_key]):
        return func.HttpResponse(
            "Storage account configuration error.",
            status_code=500
        )
        
    try: 
        permissions = BlobSasPermissions(write=True, create=True)
        expiry_time = datetime.now(timezone.utc) + timedelta(minutes=15)
        
        sas_token = generate_blob_sas(
            account_name=account_name,
            container_name=container_name,
            blob_name=blob_name,
            account_key=account_key,
            permission=permissions,
            expiry=expiry_time
        )
        
        sas_url = f"https://{account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"
        
        return func.HttpResponse(
            json.dumps({"uploadUrl": sas_url}),
            mimetype="application/json",
            status_code=200
        )
        
    except Exception as e:
        logging.error(f"Error generating SAS token: {e}")
        return func.HttpResponse(
            "Error generating SAS token.",
            status_code=500
        )
    
