import json
import logging
import os
import uuid
import re
import time
import azure.functions as func
import psycopg2
from psycopg2.extras import execute_values
from src.database import (
    get_mongo_db,
    get_student_assignments,
    get_latest_student_assignments,
    get_staff_document,
    store_document,
    store_generated_questions,
)
from src.processor import process_blob_stream, extract_batch_metadata, BlobProcessingError
from src.generator import generate_questions_logic, regenerate_questions_logic, generate_feedback
from src.practice import handle_viva_message, start_viva_session
from azure.storage.blob import generate_blob_sas, BlobSasPermissions
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from datetime import datetime, timedelta, timezone

app = func.FunctionApp()
_QUESTION_QUEUE_NAME = "iviva-question-generation"
_QUEUE_READY_RETRIES = max(1, int(os.environ.get("QUEUE_READY_RETRIES", "3")))
_QUEUE_READY_DELAY_SEC = max(0.0, float(os.environ.get("QUEUE_READY_DELAY_SEC", "2")))


def _normalize_assignment(value: str) -> str:
    # Normalize assignment labels like "Assessment 1", "Assessment-1", "Assessment_1".
    return re.sub(r"[\s_]+", "-", (value or "").strip().lower())


def _normalize_meta(value: str) -> str:
    return (value or "").strip().lower()


def _normalize_session(value: str) -> str:
    # Keep session metadata aligned across seed payloads and blob-name extraction.
    return re.sub(r"[\s_]+", "-", (value or "").strip().lower())

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
    
@app.route(route="generate_feedback", auth_level=func.AuthLevel.FUNCTION)
def generate_iviva_feedback(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('HTTP Trigger: Processing IVIVA Feedback Generation.')
    
    try: 
        req_body = req.get_json()
        unit_code = req_body.get('unit_code')
        session_year = req_body.get('session_year')
        assignment = req_body.get('assignment')
        questions = req_body.get('questions')
        answers = req_body.get('answers')
        
    except ValueError: 
        return func.HttpResponse(
            "Invalid request body. Please provide JSON with unit_code, session_year, assignment, questions, and answers.",
            status_code=400
        )
        
    try:
        feedback = generate_feedback(unit_code, session_year, assignment, questions, answers)
        return func.HttpResponse(
            json.dumps({"feedback": feedback}),
            mimetype="application/json",
            status_code=200
        )
        
    except ValueError as ve:
        return func.HttpResponse(str(ve), status_code=404)
    

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
            "Invalid request body. Please provide JSON with student_id, unit_code, session_year, and optional assignment/file content.",
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

    unit_code = _normalize_meta(req_body.get("unit_code") or req_body.get("unitCode"))
    assignment = _normalize_assignment(req_body.get("assignment"))
    session_year = _normalize_session(req_body.get("session_year") or req_body.get("sessionYear"))
    staff_id = _normalize_meta(req_body.get("staff_id") or req_body.get("staffId"))
    seed_questions = req_body.get("seed_questions") or req_body.get("seedQuestions")
    alternate_questions = req_body.get("alternate_questions") or req_body.get("alternateQuestions")
    seed_items = req_body.get("seed_items") or req_body.get("seedItems")

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
    if alternate_questions is not None and not isinstance(alternate_questions, list):
        return func.HttpResponse(
            "alternate_questions must be a list of strings when provided.",
            status_code=400,
        )
    if seed_items is not None and not isinstance(seed_items, list):
        return func.HttpResponse(
            "seed_items must be a list when provided.",
            status_code=400,
        )

    cleaned_questions = []
    cleaned_alternates = []
    if isinstance(seed_items, list) and seed_items:
        for item in seed_items:
            if not isinstance(item, dict):
                continue
            primary = str(item.get("question") or "").strip()
            alt = str(item.get("alternate_question") or "").strip()
            if primary:
                cleaned_questions.append(primary)
            if alt:
                cleaned_alternates.append(alt)
    else:
        cleaned_questions = [str(q).strip() for q in seed_questions if str(q).strip()]
        cleaned_alternates = [str(q).strip() for q in (alternate_questions or []) if str(q).strip()]

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
        "staff_id": staff_id or None,
        "alternate_questions": cleaned_alternates,
        "seed_items": seed_items if isinstance(seed_items, list) else None,
    }

    try:
        db = get_mongo_db()
        collection = db["iviva-staff-seed-questions"]
        store_document(collection, metadata, content, source_blob="SeedQuestionsUpload")
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
        if not myblob.name:
            raise ValueError("Blob name is missing")
        process_blob_stream(blob_content, myblob.name, target_collection)

        logging.info(f"Successfully saved to {target_collection_name}")

    except Exception as e:
        logging.error(
            f"Error processing {myblob.name}: {str(e)}", exc_info=True)
        raise


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


def _get_postgres_connection():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL is not configured.")
    return psycopg2.connect(database_url)


def _create_question_set(
    cur,
    unit_code: str,
    assignment: str,
    session_year: str,
    staff_id: str | None = None,
) -> str:
    name = f"{unit_code}_{assignment}_{session_year}"
    question_set_id = str(uuid.uuid4())
    if staff_id:
        cur.execute(
            'INSERT INTO "PersonalisedQuestionSets" '
            '("questionSetId", "name", "unitCode", "assessmentName", "staffId") '
            'VALUES (%s, %s, %s, %s, %s) RETURNING "questionSetId"',
            (question_set_id, name, unit_code, assignment, staff_id),
        )
        return cur.fetchone()[0]

    cur.execute(
        'INSERT INTO "PersonalisedQuestionSets" '
        '("questionSetId", "name", "unitCode", "assessmentName") '
        'VALUES (%s, %s, %s, %s) RETURNING "questionSetId"',
        (question_set_id, name, unit_code, assignment),
    )
    return cur.fetchone()[0]


def _get_or_create_question_set(
    cur,
    unit_code: str,
    assignment: str,
    session_year: str,
    staff_id: str | None = None,
) -> str:
    name = f"{unit_code}_{assignment}_{session_year}"
    # Serialize get/create per logical set key to prevent duplicate rows under concurrent queue workers.
    lock_key = f"{unit_code}|{assignment}|{session_year}"
    cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (lock_key,))

    cur.execute(
        'SELECT "questionSetId", "staffId" FROM "PersonalisedQuestionSets" '
        'WHERE "unitCode" = %s AND "assessmentName" = %s AND "name" = %s '
        'ORDER BY "createdAt" DESC LIMIT 1',
        (unit_code, assignment, name),
    )
    row = cur.fetchone()
    if row:
        question_set_id, existing_staff_id = row[0], row[1]
        if staff_id and not existing_staff_id:
            cur.execute(
                'UPDATE "PersonalisedQuestionSets" SET "staffId" = %s WHERE "questionSetId" = %s',
                (staff_id, question_set_id),
            )
        return question_set_id

    return _create_question_set(
        cur,
        unit_code=unit_code,
        assignment=assignment,
        session_year=session_year,
        staff_id=staff_id,
    )


def _set_question_set_processing_status(
    unit_code: str,
    assignment: str,
    session_year: str,
    staff_id: str | None,
    expected_submission_count: int,
) -> str:
    with _get_postgres_connection() as conn:
        with conn.cursor() as cur:
            question_set_id = _get_or_create_question_set(
                cur,
                unit_code=unit_code,
                assignment=assignment,
                session_year=session_year,
                staff_id=staff_id,
            )
            cur.execute(
                'UPDATE "PersonalisedQuestionSets" '
                'SET "status" = %s, "expectedStudentCount" = %s '
                'WHERE "questionSetId" = %s',
                ("PROCESSING", expected_submission_count, question_set_id),
            )
            conn.commit()
            return question_set_id


def _append_question_set_error(
    cur,
    unit_code: str | None,
    assignment: str | None,
    session_year: str | None,
    message: str,
    question_set_id: str | None = None,
):
    unit_code = _normalize_meta(unit_code or "")
    assignment = _normalize_assignment(assignment or "")
    session_year = _normalize_session(session_year or "")
    if not question_set_id:
        logging.warning(
            "Skipping errorMessage update due to missing question_set_id: %s/%s/%s",
            unit_code,
            assignment,
            session_year,
        )
        return

    timestamp = datetime.now(timezone.utc).isoformat()
    formatted = f"[{timestamp}] {message}"

    cur.execute(
        'UPDATE "PersonalisedQuestionSets" '
        'SET "errorMessage" = CASE '
        'WHEN "errorMessage" IS NULL OR "errorMessage" = %s THEN %s '
        'ELSE "errorMessage" || E\'\\n\' || %s END, '
        '"status" = %s '
        'WHERE "questionSetId" = %s',
        ("", formatted, formatted, "UNSUCCESSFUL", question_set_id),
    )


def _try_mark_question_set_completed(
    cur,
    question_set_id: str,
    expected_submission_count: int | None,
):
    if not expected_submission_count or expected_submission_count <= 0:
        return

    cur.execute(
        'SELECT COUNT(DISTINCT "studentId") '
        'FROM "PersonalisedQuestions" '
        'WHERE "questionSetId" = %s AND "studentId" IS NOT NULL',
        (question_set_id,),
    )
    row = cur.fetchone()
    completed_count = int(row[0] or 0) if row else 0
    if completed_count < expected_submission_count:
        return

    cur.execute(
        'UPDATE "PersonalisedQuestionSets" '
        'SET "status" = %s '
        'WHERE "questionSetId" = %s AND "status" <> %s',
        ("COMPLETED", question_set_id, "COMPLETED"),
    )
    if cur.rowcount:
        logging.info(
            "Question set %s marked COMPLETED (%s/%s students).",
            question_set_id,
            completed_count,
            expected_submission_count,
        )


def _store_questions_postgres(
    student_id: str,
    unit_code: str,
    assignment: str,
    session_year: str,
    staff_id: str | None,
    questions: list,
    reference: list,
    question_set_id: str | None = None,
    alternate_questions: list | None = None,
    expected_submission_count: int | None = None,
):
    if not questions:
        return

    reference_list = reference if isinstance(reference, list) else []
    alternate_list = alternate_questions if isinstance(alternate_questions, list) else []
    rows = []
    for idx, question in enumerate(questions):
        if question is None:
            continue
        reference_text = reference_list[idx] if idx < len(reference_list) else None
        alternate_text = str(alternate_list[idx]).strip() if idx < len(alternate_list) and alternate_list[idx] is not None else None
        rows.append((str(uuid.uuid4()), str(question), reference_text, student_id, alternate_text))

    if not rows:
        return

    def _attempt_insert(effective_staff_id: str | None, include_student_id: bool):
        with _get_postgres_connection() as conn:
            with conn.cursor() as cur:
                resolved_question_set_id = question_set_id or _get_or_create_question_set(
                    cur,
                    unit_code=unit_code,
                    assignment=assignment,
                    session_year=session_year,
                    staff_id=effective_staff_id,
                )
                violations = []
                if effective_staff_id:
                    cur.execute(
                        'SELECT 1 FROM "Users" WHERE "schoolId" = %s OR "userId"::text = %s',
                        (effective_staff_id, effective_staff_id),
                    )
                    if cur.fetchone() is None:
                        violations.append(
                            "Postgres FK violation on staffId (%s)." % effective_staff_id
                        )
                if include_student_id:
                    cur.execute(
                        'SELECT 1 FROM "Users" WHERE "schoolId" = %s OR "userId"::text = %s',
                        (student_id, student_id),
                    )
                    if cur.fetchone() is None:
                        violations.append(
                            "Postgres FK violation on studentId (%s)." % student_id
                        )
                if violations:
                    _append_question_set_error(
                        cur,
                        unit_code=unit_code,
                        assignment=assignment,
                        session_year=session_year,
                        message="\n".join(violations),
                        question_set_id=resolved_question_set_id,
                    )
                    raise ValueError("\n".join(violations))
                # Idempotent behavior: replace existing generated rows for this student/set.
                if include_student_id:
                    cur.execute(
                        'DELETE FROM "PersonalisedQuestions" WHERE "questionSetId" = %s AND "studentId" = %s',
                        (resolved_question_set_id, student_id),
                    )
                else:
                    cur.execute(
                        'DELETE FROM "PersonalisedQuestions" WHERE "questionSetId" = %s AND "studentId" IS NULL',
                        (resolved_question_set_id,),
                    )

                rows_with_set = [
                    (
                        question_id,
                        question_text,
                        reference_text,
                        resolved_question_set_id,
                        student_id if include_student_id else None,
                        alternate_text,
                    )
                    for question_id, question_text, reference_text, student_id, alternate_text in rows
                ]
                execute_values(
                    cur,
                    'INSERT INTO "PersonalisedQuestions" '
                    '("questionId", "questionText", "referenceText", "questionSetId", "studentId", "alternateQuestion") VALUES %s',
                    rows_with_set,
                )

                _try_mark_question_set_completed(
                    cur,
                    question_set_id=resolved_question_set_id,
                    expected_submission_count=expected_submission_count,
                )

                # For summative/normal flow, ensure each student has one session for this question set.
                # Idempotent: only create when no existing session links this student to the set.
                if include_student_id:
                    cur.execute(
                        'WITH matched_user AS ('
                        '  SELECT "userId" '
                        '  FROM "Users" '
                        '  WHERE "schoolId" = %s OR "userId"::text = %s '
                        '  LIMIT 1'
                        ') '
                        'INSERT INTO "Sessions" ("sessionId", "studentId", "questionSetId", "status", "remainingAttempt") '
                        'SELECT %s, m."userId", %s, %s, %s '
                        'FROM matched_user m '
                        'WHERE NOT EXISTS ('
                        '  SELECT 1 FROM "Sessions" s '
                        '  WHERE s."studentId" = m."userId" AND s."questionSetId" = %s'
                        ')',
                        (
                            student_id,
                            student_id,
                            str(uuid.uuid4()),
                            resolved_question_set_id,
                            "READY_TO_START",
                            1,
                            resolved_question_set_id,
                        ),
                    )
            conn.commit()

    effective_staff_id = staff_id
    include_student_id = True
    for _ in range(3):
        try:
            _attempt_insert(
                effective_staff_id=effective_staff_id,
                include_student_id=include_student_id,
            )
            return
        except psycopg2.Error as e:
            constraint_name = getattr(getattr(e, "diag", None), "constraint_name", "")
            is_fk_violation = e.pgcode == "23503"
            if (
                is_fk_violation
                and "PersonalisedQuestionSets_staffId_fkey" in (constraint_name or "")
                and effective_staff_id
            ):
                with _get_postgres_connection() as conn:
                    with conn.cursor() as cur:
                        _append_question_set_error(
                            cur,
                            unit_code=unit_code,
                            assignment=assignment,
                            session_year=session_year,
                            message=(
                                "Postgres FK violation on staffId (%s). Aborting insert."
                                % effective_staff_id
                            ),
                            question_set_id=question_set_id,
                        )
                    conn.commit()
                raise
            if (
                is_fk_violation
                and "PersonalisedQuestions_studentId_fkey" in (constraint_name or "")
                and include_student_id
            ):
                with _get_postgres_connection() as conn:
                    with conn.cursor() as cur:
                        _append_question_set_error(
                            cur,
                            unit_code=unit_code,
                            assignment=assignment,
                            session_year=session_year,
                            message=(
                                "Postgres FK violation on studentId (%s). Aborting insert."
                                % student_id
                            ),
                            question_set_id=question_set_id,
                        )
                    conn.commit()
                raise
            raise


def _has_postgres_questions(
    student_id: str,
    unit_code: str,
    assignment: str,
    session_year: str,
) -> bool:
    set_name = f"{unit_code}_{assignment}_{session_year}"
    with _get_postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'SELECT 1 '
                'FROM "PersonalisedQuestions" pq '
                'JOIN "PersonalisedQuestionSets" qs ON qs."questionSetId" = pq."questionSetId" '
                'WHERE pq."studentId" = %s '
                '  AND qs."unitCode" = %s '
                '  AND qs."assessmentName" = %s '
                '  AND qs."name" = %s '
                'LIMIT 1',
                (student_id, unit_code, assignment, set_name),
            )
            return cur.fetchone() is not None


def _enqueue_generation_jobs(unit_code: str, assignment: str, session_year: str):
    unit_code = _normalize_meta(unit_code)
    assignment = _normalize_assignment(assignment)
    session_year = _normalize_session(session_year)
    staff_ready = False
    for attempt in range(1, _QUEUE_READY_RETRIES + 1):
        if _staff_docs_ready(unit_code, assignment, session_year):
            staff_ready = True
            break
        if attempt < _QUEUE_READY_RETRIES:
            logging.info(
                f"Staff docs not ready for {unit_code}_{assignment}_{session_year}, "
                f"retrying in {_QUEUE_READY_DELAY_SEC:.1f}s ({attempt}/{_QUEUE_READY_RETRIES})."
            )
            time.sleep(_QUEUE_READY_DELAY_SEC)

    if not staff_ready:
        logging.info(
            f"Staff docs not ready for {unit_code}_{assignment}_{session_year}, skipping queue."
        )
        return

    connection_string = os.environ.get("SERVICEBUS_CONNECTION")
    if not connection_string:
        raise ValueError("SERVICEBUS_CONNECTION is not configured.")
    student_cursor = get_latest_student_assignments(
        unit_code=unit_code,
        session_year=session_year,
        assignment=assignment,
    )
    seed_doc = get_staff_document(
        collection_name="iviva-staff-seed-questions",
        unit_code=unit_code,
        session_year=session_year,
        assignment=assignment,
    )
    staff_id = _normalize_meta((seed_doc or {}).get("staff_id") or "") if seed_doc else None
    alternate_questions = (seed_doc or {}).get("alternate_questions") if seed_doc else []

    student_ids: list[str] = []
    for doc in student_cursor:
        student_id = _normalize_meta(doc.get("student_id"))
        if student_id:
            student_ids.append(student_id)

    expected_submission_count = len(student_ids)
    if expected_submission_count == 0:
        logging.warning(
            f"No student submissions found for {unit_code}_{assignment}_{session_year}; skipping queue."
        )
        return

    try:
        question_set_id = _set_question_set_processing_status(
            unit_code=unit_code,
            assignment=assignment,
            session_year=session_year,
            staff_id=staff_id,
            expected_submission_count=expected_submission_count,
        )
    except Exception as e:
        logging.error(f"Failed to initialize question set status: {e}", exc_info=True)
        raise

    enqueued = 0
    with ServiceBusClient.from_connection_string(connection_string) as client:
        with client.get_queue_sender(_QUESTION_QUEUE_NAME) as sender:
            for submission_index, student_id in enumerate(student_ids, start=1):
                # Comment out to newly generate a question set
                # if has_generated_questions(student_id, unit_code, assignment, session_year):
                #     continue
                payload = {
                    "student_id": student_id,
                    "unit_code": unit_code,
                    "assignment": assignment,
                    "session_year": session_year,
                    "staff_id": staff_id,
                    "alternate_questions": alternate_questions,
                    "question_set_id": question_set_id,
                    "expected_submission_count": expected_submission_count,
                    "submission_index": submission_index,
                }
                sender.send_messages(ServiceBusMessage(json.dumps(payload)))
                enqueued += 1

    logging.info(
        f"Enqueued {enqueued} question generation jobs for {unit_code}_{assignment}_{session_year}."
    )


@app.function_name(name="StudentAssignmentsUpload")
@app.blob_trigger(
    arg_name="myblob", 
    path="iviva-student-assignments/{name}", 
    connection="AzureWebJobsStorage")
def student_assignments_upload(myblob: func.InputStream):
    """
    Azure Function triggered by Blob storage events via Event Grid.
    Processes the blob and saves to 'assignments' collection.
    """
    try:
        _handle_blob_event(myblob, target_collection_name="iviva-student-assignments")
    except BlobProcessingError as be:
        metadata = be.metadata
        unit_code = metadata.get("unit_code") or ""
        assignment = metadata.get("assignment") or ""
        session_year = metadata.get("session_year") or ""
        
        with _get_postgres_connection() as conn:
            with conn.cursor() as cur:
                question_set_id = _get_or_create_question_set(
                    cur, unit_code, assignment, session_year
                )
                _append_question_set_error(
                    cur,
                    unit_code=unit_code,
                    assignment=assignment,
                    session_year=session_year,
                    message=str(be),
                    question_set_id=question_set_id,
                )
            conn.commit()
        return
    metadata = extract_batch_metadata(myblob.name or "")
    _enqueue_generation_jobs(
        unit_code=metadata["unit_code"],
        assignment=metadata["assignment"],
        session_year=metadata["session_year"],
    )
    
# Upload assessment brief
@app.function_name(name="BriefUpload")
@app.blob_trigger(arg_name="myblob", 
                  path="iviva-staff-assessment-brief/{name}", 
                  connection="AzureWebJobsStorage")
def brief_upload(myblob: func.InputStream):
    try:
        _handle_blob_event(
            myblob, target_collection_name="iviva-staff-assessment-brief")
    except BlobProcessingError as be:
        metadata = be.metadata
        unit_code = metadata.get("unit_code") or ""
        assignment = metadata.get("assignment") or ""
        session_year = metadata.get("session_year") or ""
        
        with _get_postgres_connection() as conn:
            with conn.cursor() as cur:
                question_set_id = _get_or_create_question_set(
                    cur, unit_code, assignment, session_year
                )
                _append_question_set_error(
                    cur,
                    unit_code=unit_code,
                    assignment=assignment,
                    session_year=session_year,
                    message=str(be),
                    question_set_id=question_set_id,
                )
            conn.commit()
        return
    
# Upload assessment rubrics
@app.function_name(name="RubricUpload")
@app.blob_trigger(arg_name="myblob", 
                  path="iviva-staff-assessment-rubrics/{name}", 
                  connection="AzureWebJobsStorage")
def rubric_upload(myblob: func.InputStream):
    try:
        _handle_blob_event(
            myblob, target_collection_name="iviva-staff-assessment-rubrics")
    except BlobProcessingError as be:
        metadata = be.metadata
        unit_code = metadata.get("unit_code") or ""
        assignment = metadata.get("assignment") or ""
        session_year = metadata.get("session_year") or ""
        
        with _get_postgres_connection() as conn:
            with conn.cursor() as cur:
                question_set_id = _get_or_create_question_set(
                    cur, unit_code, assignment, session_year
                )
                _append_question_set_error(
                    cur,
                    unit_code=unit_code,
                    assignment=assignment,
                    session_year=session_year,
                    message=str(be),
                    question_set_id=question_set_id,
                )
            conn.commit()
        return


# ==========================================
#  2B. Queue Trigger: Question Generation
# ==========================================
@app.function_name(name="QuestionGenerationQueue")
@app.service_bus_queue_trigger(
    arg_name="msg",
    queue_name="iviva-question-generation",
    connection="SERVICEBUS_CONNECTION",
)
def question_generation_queue(msg: func.ServiceBusMessage):
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
    staff_id = payload.get("staff_id")
    alternate_questions = payload.get("alternate_questions") or []
    question_set_id = payload.get("question_set_id")
    expected_submission_count = payload.get("expected_submission_count")
    submission_index = payload.get("submission_index")

    try:
        expected_submission_count = int(expected_submission_count) if expected_submission_count is not None else None
    except (TypeError, ValueError):
        expected_submission_count = None

    try:
        submission_index = int(submission_index) if submission_index is not None else None
    except (TypeError, ValueError):
        submission_index = None

    if not all([student_id, unit_code, assignment, session_year]):
        logging.error("Queue payload missing required fields.")
        return

    if submission_index and expected_submission_count:
        logging.info(
            "Question generation processing student %s/%s (student_id=%s).",
            submission_index,
            expected_submission_count,
            student_id,
        )
    else:
        logging.info("Question generation processing student_id=%s.", student_id)

    try:
        result = generate_questions_logic(
            student_id=student_id,
            unit_code=unit_code,
            session=session_year,
            assignment=assignment,
        )
        questions = result.get("questions", [])
        reference = result.get("reference", [])

        try:
            _store_questions_postgres(
                student_id=student_id,
                unit_code=unit_code,
                assignment=assignment,
                session_year=session_year,
                staff_id=staff_id,
                question_set_id=question_set_id,
                questions=questions,
                reference=reference,
                alternate_questions=alternate_questions,
                expected_submission_count=expected_submission_count,
            )
        except Exception as e:
            logging.error(f"Postgres insert failed: {e}", exc_info=True)
            # Do not mark as generated when Postgres persistence fails.
            # This keeps queue retries available instead of permanently skipping this student.
            raise
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
        "iviva-staff-seed-questions",
        "iviva-staff-exemplar-assignments",
        "iviva-staff-viva-rubrics",
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
    
    # Type narrowing: assert values are not None after guard
    assert account_name is not None and account_key is not None
        
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
    
