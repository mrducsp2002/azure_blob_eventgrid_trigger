import os
import logging
from datetime import datetime, timezone
from pymongo import MongoClient

# Global variable for connection pooling
_mongo_db = None  

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

    # 1. Base Document (Fields shared by EVERYONE)
    document = {
        "unit_code": metadata['unit_code'],
        "assignment": metadata['assignment'],
        "session_year": metadata['session_year'],
        "content": content,
        "source_blob": source_blob,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    # 2. Determine ID and Role based on Student ID presence
    student_id = metadata.get('student_id')

    if student_id:
        doc_id = f"{student_id}_{metadata['unit_code']}_{metadata['assignment']}_{metadata['session_year']}"
        document["student_id"] = student_id  # Add student_id
    else:
        doc_id = f"{metadata['unit_code']}_{metadata['assignment']}_{metadata['session_year']}"
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
            "student_id": student_id,
            "unit_code": unit_code,
            "session_year": session_year,
            "assignment": assignment,
        }
    )


def get_student_assignments(unit_code: str, session_year: str, assignment: str):
    db = get_mongo_db()
    return db["iviva-student-assignments"].find(
        {
            "unit_code": unit_code,
            "session_year": session_year,
            "assignment": assignment,
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
    document = {
        "unit_code": metadata["unit_code"],
        "assignment": metadata["assignment"],
        "session_year": metadata["session_year"],
        "student_id": metadata["student_id"],
        "questions": questions,
        "reference": reference,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    doc_id = f"{metadata['student_id']}_{metadata['unit_code']}_{metadata['assignment']}_{metadata['session_year']}"
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
    doc_id = f"{student_id}_{unit_code}_{assignment}_{session_year}"
    return db["iviva-generated-questions"].find_one({"_id": doc_id}) is not None


def get_staff_document(collection_name: str, unit_code: str, session_year: str, assignment: str):
    db = get_mongo_db()
    return db[collection_name].find_one(
        {
            "unit_code": unit_code,
            "session_year": session_year,
            "assignment": assignment,
        }
    )
