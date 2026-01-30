import json
import logging
import os
import azure.functions as func
from src.database import (
    get_mongo_db,
    get_student_assignments,
    get_staff_document,
    has_generated_questions,
    store_document,
    store_generated_questions,
)
from src.processor import process_blob_stream, extract_batch_metadata
from src.generator import generate_questions_logic, regenerate_questions_logic
from src.practice import handle_viva_message, start_viva_session
from azure.storage.blob import generate_blob_sas, BlobSasPermissions
from azure.storage.queue import QueueClient
from datetime import datetime, timedelta, timezone

app = func.FunctionApp()
_QUESTION_QUEUE_NAME = "iviva-question-generation"

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
#  1C. HTTP API: Seed Questions Upload
# ==========================================
@app.route(route="upload_seed_questions", auth_level=func.AuthLevel.FUNCTION)
def upload_seed_questions(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("HTTP Trigger: Uploading Seed Questions.")

    try:
        req_body = req.get_json()
    except ValueError:
        return func.HttpResponse(
            "Invalid request body. Provide JSON with unit_code, assignment, session_year, seed_questions.",
            status_code=400,
        )

    unit_code = req_body.get("unit_code") or req_body.get("unitCode")
    assignment = req_body.get("assignment")
    session_year = req_body.get("session_year") or req_body.get("sessionYear")
    seed_questions = req_body.get("seed_questions") or req_body.get("seedQuestions")

    if not all([unit_code, assignment, session_year, seed_questions]):
        return func.HttpResponse(
            "Missing required parameters. Provide unit_code, assignment, session_year, seed_questions.",
            status_code=400,
        )

    if not isinstance(seed_questions, list):
        return func.HttpResponse(
            "seed_questions must be a list of strings.",
            status_code=400,
        )

    cleaned_questions = [str(q).strip() for q in seed_questions if str(q).strip()]
    if not cleaned_questions:
        return func.HttpResponse(
            "seed_questions cannot be empty.",
            status_code=400,
        )

    content = "\n".join(
        [f"{idx + 1}. {question}" for idx, question in enumerate(cleaned_questions)]
    )
    metadata = {
        "unit_code": unit_code,
        "assignment": assignment,
        "session_year": session_year,
    }

    try:
        db = get_mongo_db()
        collection = db["iviva-staff-seed-questions"]
        store_document(collection, metadata, content, source_blob="SeedQuestionsUpload")
        _enqueue_generation_jobs(
            unit_code=unit_code,
            assignment=assignment,
            session_year=session_year,
        )
        return func.HttpResponse(
            json.dumps({"status": "ok"}),
            mimetype="application/json",
            status_code=200,
        )
    except Exception as e:
        logging.error(f"Error uploading seed questions: {e}")
        return func.HttpResponse(
            "Error uploading seed questions.",
            status_code=500,
        )

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


def _get_queue_client() -> QueueClient:
    connection_string = os.environ.get("AzureWebJobsStorage")
    if not connection_string:
        raise ValueError("AzureWebJobsStorage is not configured.")
    queue_client = QueueClient.from_connection_string(
        conn_str=connection_string,
        queue_name=_QUESTION_QUEUE_NAME,
    )
    try:
        queue_client.create_queue()
    except Exception:
        pass
    return queue_client


def _staff_docs_ready(unit_code: str, assignment: str, session_year: str) -> bool:
    brief = get_staff_document(
        collection_name="iviva-staff-assessment-brief",
        unit_code=unit_code,
        session_year=session_year,
        assignment=assignment,
    )
    rubric = get_staff_document(
        collection_name="iviva-staff-assessment-rubrics",
        unit_code=unit_code,
        session_year=session_year,
        assignment=assignment,
    )
    seed = get_staff_document(
        collection_name="iviva-staff-seed-questions",
        unit_code=unit_code,
        session_year=session_year,
        assignment=assignment,
    )
    return bool(brief and rubric and seed)


def _enqueue_generation_jobs(unit_code: str, assignment: str, session_year: str):
    if not _staff_docs_ready(unit_code, assignment, session_year):
        logging.info(
            f"Staff docs not ready for {unit_code}_{assignment}_{session_year}, skipping queue."
        )
        return

    queue_client = _get_queue_client()
    student_cursor = get_student_assignments(
        unit_code=unit_code,
        session_year=session_year,
        assignment=assignment,
    )

    enqueued = 0
    for doc in student_cursor:
        student_id = doc.get("student_id")
        if not student_id:
            continue
        if has_generated_questions(student_id, unit_code, assignment, session_year):
            continue
        payload = {
            "student_id": student_id,
            "unit_code": unit_code,
            "assignment": assignment,
            "session_year": session_year,
        }
        queue_client.send_message(json.dumps(payload))
        enqueued += 1

    logging.info(
        f"Enqueued {enqueued} question generation jobs for {unit_code}_{assignment}_{session_year}."
    )


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
    metadata = extract_batch_metadata(myblob.name)
    _enqueue_generation_jobs(
        unit_code=metadata["unit_code"],
        assignment=metadata["assignment"],
        session_year=metadata["session_year"],
    )
    
# Upload assessment brief
@app.function_name(name="BriefUpload")
@app.blob_trigger(arg_name="myblob", 
                  path="iviva-staff-assessment-brief/{name}", 
                  source="EventGrid",
                  connection="AzureWebJobsStorage")
def brief_upload(myblob: func.InputStream):
    _handle_blob_event(
        myblob, target_collection_name="iviva-staff-assessment-brief")
    metadata = extract_batch_metadata(myblob.name)
    _enqueue_generation_jobs(
        unit_code=metadata["unit_code"],
        assignment=metadata["assignment"],
        session_year=metadata["session_year"],
    )
    
# Upload assessment rubrics
@app.function_name(name="RubricUpload")
@app.blob_trigger(arg_name="myblob", 
                  path="iviva-staff-assessment-rubrics/{name}", 
                  source="EventGrid",
                  connection="AzureWebJobsStorage")
def rubric_upload(myblob: func.InputStream):
    _handle_blob_event(
        myblob, target_collection_name="iviva-staff-assessment-rubrics")
    metadata = extract_batch_metadata(myblob.name)
    _enqueue_generation_jobs(
        unit_code=metadata["unit_code"],
        assignment=metadata["assignment"],
        session_year=metadata["session_year"],
    )


# ==========================================
#  2B. Queue Trigger: Question Generation
# ==========================================
@app.function_name(name="QuestionGenerationQueue")
@app.queue_trigger(
    arg_name="msg",
    queue_name=_QUESTION_QUEUE_NAME,
    connection="AzureWebJobsStorage",
)
def question_generation_queue(msg: func.QueueMessage):
    logging.info("Queue Trigger: Processing Question Generation Job.")
    raw_body = msg.get_body().decode("utf-8")
    logging.info(f"Queue trigger payload: {raw_body}")
    try:
        payload = json.loads(msg.get_body().decode("utf-8"))
    except Exception as e:
        logging.error(f"Invalid queue payload: {e}")
        return

    student_id = payload.get("student_id")
    unit_code = payload.get("unit_code")
    assignment = payload.get("assignment")
    session_year = payload.get("session_year")

    if not all([student_id, unit_code, assignment, session_year]):
        logging.error("Queue payload missing required fields.")
        return

    if has_generated_questions(student_id, unit_code, assignment, session_year):
        logging.info(
            f"Questions already generated for {student_id}_{unit_code}_{assignment}_{session_year}."
        )
        return

    try:
        result = generate_questions_logic(
            student_id=student_id,
            unit_code=unit_code,
            session=session_year,
            assignment=assignment,
        )
        questions = result.get("questions", [])
        reference = result.get("reference", [])

        db = get_mongo_db()
        collection = db["iviva-staff-generated-questions"]
        store_generated_questions(
            collection,
            {
                "student_id": student_id,
                "unit_code": unit_code,
                "assignment": assignment,
                "session_year": session_year,
            },
            questions,
            reference,
        )
    except ValueError as ve:
        logging.error(f"Question generation failed: {ve}")
    except Exception as e:
        logging.error(f"Unexpected generation error: {e}", exc_info=True)
        raise



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
        "iviva-staff-assessment-rubrics",
        "iviva-staff-seed-questions"
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
    

