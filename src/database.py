import os
import logging
import re
from datetime import datetime, timezone
from pymongo import MongoClient

# Global variable for connection pooling
_mongo_db = None  


def _norm(value):
    return str(value).strip().lower() if value is not None else None

def get_mongo_db():
    global _mongo_db
    if _mongo_db is None:
        mongo_str = os.environ["MONGO_CONNECTION_STRING"]
        db_name = os.environ["DB_NAME"]
        client = MongoClient(mongo_str)
        _mongo_db = client[db_name]  # Store the Database
    return _mongo_db


def store_document(collection, metadata: dict, content: str, source_blob: str):
    """
    Upserts a document (Student Submission OR Assessment Brief) into MongoDB.
    
    Expected metadata:
    - unit_code
    - assignment
    - session_year
    - student_id (Optional - Only for students)
    """

    unit_code = _norm(metadata.get('unit_code')) or "unknown_unit"
    assignment = _norm(metadata.get('assignment')) or "unknown_ass"
    session_year = _norm(metadata.get('session_year')) or "unknown_year"
    student_id = _norm(metadata.get('student_id'))

    # 1. Base Document (Fields shared by EVERYONE)
    document = {
        "unit_code": unit_code,
        "assignment": assignment,
        "session_year": session_year,
        "content": content,
        "source_blob": source_blob,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    # 2. Determine ID and Role based on Student ID presence
    if student_id:
        doc_id = f"{student_id}_{unit_code}_{assignment}_{session_year}"
        document["student_id"] = student_id  # Add student_id
    else:
        doc_id = f"{unit_code}_{assignment}_{session_year}"
    document["_id"] = doc_id

    # 3. Upsert
    try:
        collection.replace_one(
            filter={"_id": doc_id},
            replacement=document,
            upsert=True
        )
        logging.info(f"Stored for: {doc_id}")
    except Exception as e:
        logging.error(f"Database error for {doc_id}: {e}")


def get_student_assignment(student_id: str, unit_code: str, session_year: str, assignment: str):
    db = get_mongo_db()
    return db["iviva-student-assignments"].find_one(
        {
            "student_id": _norm(student_id),
            "unit_code": _norm(unit_code),
            "session_year": _norm(session_year),
            "assignment": _norm(assignment),
        }
    )


def get_student_assignments(unit_code: str, session_year: str, assignment: str):
    db = get_mongo_db()
    return db["iviva-student-assignments"].find(
        {
            "unit_code": _norm(unit_code),
            "session_year": _norm(session_year),
            "assignment": _norm(assignment),
        },
        {"student_id": 1},
    )


def store_generated_questions(collection, metadata: dict, questions: list, reference: list):
    """
    Upserts generated questions for a student.

    Expected metadata:
    - unit_code
    - assignment
    - session_year
    - student_id
    """
    unit_code = _norm(metadata.get("unit_code"))
    assignment = _norm(metadata.get("assignment"))
    session_year = _norm(metadata.get("session_year"))
    student_id = _norm(metadata.get("student_id"))

    document = {
        "unit_code": unit_code,
        "assignment": assignment,
        "session_year": session_year,
        "student_id": student_id,
        "questions": questions,
        "reference": reference,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    doc_id = f"{student_id}_{unit_code}_{assignment}_{session_year}"
    document["_id"] = doc_id

    try:
        collection.replace_one(
            filter={"_id": doc_id},
            replacement=document,
            upsert=True,
        )
        logging.info(f"Stored generated questions for: {doc_id}")
    except Exception as e:
        logging.error(f"Database error for {doc_id}: {e}")


def has_generated_questions(student_id: str, unit_code: str, assignment: str, session_year: str) -> bool:
    db = get_mongo_db()
    doc_id = f"{_norm(student_id)}_{_norm(unit_code)}_{_norm(assignment)}_{_norm(session_year)}"
    return db["iviva-staff-generated-questions"].find_one({"_id": doc_id}) is not None


def get_staff_document(collection_name: str, unit_code: str, session_year: str, assignment: str | None = None):
    db = get_mongo_db()
    normalized_assignment = _norm(assignment) if assignment else None
    query = {
        "unit_code": _norm(unit_code),
        "session_year": _norm(session_year),
    }
    if normalized_assignment:
        query["assignment"] = normalized_assignment

    # If assignment is not provided, use the most recent guidance doc for the unit/session.
    if not assignment:
        return db[collection_name].find_one(query, sort=[("timestamp", -1)])

    doc = db[collection_name].find_one(query)
    if doc:
        return doc

    # Fallback for assignment variants, e.g. "Assessment 1" vs "Assessment-1".
    if normalized_assignment:
        tokens = [t for t in re.split(r"[-_\s]+", normalized_assignment.strip()) if t]
        if tokens:
            assignment_pattern = r"^" + r"[-_\s]*".join(re.escape(t) for t in tokens) + r"$"
            fuzzy_query = {
                "unit_code": unit_code,
                "session_year": session_year,
                "assignment": {"$regex": assignment_pattern, "$options": "i"},
            }
            return db[collection_name].find_one(fuzzy_query)

    return None
