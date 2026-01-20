import json
import os
import re
import uuid
from typing import Any, Dict, List, Optional, Tuple

from azure.storage.blob import BlobServiceClient
from openai import AzureOpenAI

from src.database import get_student_assignment
from src.generator import generate_questions_logic
from src.parsers import decode_file_content


GUIDANCE_CONTAINER = "iviva-staff-assessment-brief"
GUIDANCE_BLOB = "comp1010_assignment2_s22025.pdf"

GRADING_SYSTEM_PROMPT = (
    "You are an examiner grading viva answers using only the provided document text. "
    "Given the viva questions and the student's answers, provide concise feedback grounded in the text and a score out of 10. "
    "If the text does not support an answer, call that out. "
    "Output:\n\n"
    "### FEEDBACK\n"
    "- Overall score: <int>/10\n"
    "- Summary: <one short sentence>\n"
    "- Per question: Q1 <feedback>; Q2 <feedback>; Q3 <feedback>\n\n"
    "### SOURCES\n"
    "<one source per line, with text from the retrieved chunk sources and the specific line they come from>\n"
)

_SESSIONS: Dict[str, Dict[str, Any]] = {}

_client = AzureOpenAI(
    api_version="2024-12-01-preview",
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT", "https://iviva.cognitiveservices.azure.com/"),
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
)


def _get_blob_bytes(container_name: str, blob_name: str) -> bytes:
    account_name = os.environ.get("STORAGE_ACCOUNT_NAME")
    account_key = os.environ.get("STORAGE_ACCOUNT_KEY")
    if not account_name or not account_key:
        raise ValueError("Storage account configuration error.")

    account_url = f"https://{account_name}.blob.core.windows.net"
    service_client = BlobServiceClient(account_url=account_url, credential=account_key)
    blob_client = service_client.get_blob_client(container=container_name, blob=blob_name)
    return blob_client.download_blob().readall()


def _load_guidance_text() -> str:
    guidance_bytes = _get_blob_bytes(GUIDANCE_CONTAINER, GUIDANCE_BLOB)
    return decode_file_content(GUIDANCE_BLOB, guidance_bytes)


def _extract_score(feedback: str) -> Optional[int]:
    match = re.search(r"\b([0-9]|10)/10\b", feedback)
    if match:
        val = int(match.group(1))
        if 0 <= val <= 10:
            return val
    return None


def _grade_answers(document_text: str, questions: List[str], answers: List[str]) -> Tuple[str, Optional[int]]:
    qa_text = "\n".join(
        [f"Q{i + 1}: {questions[i]}\nA{i + 1}: {answers[i]}" for i in range(len(questions))]
    )
    completion = _client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": GRADING_SYSTEM_PROMPT},
            {"role": "user", "content": f"Document:\n{document_text}\n\nQuestions and Answers:\n{qa_text}"},
        ],
    )
    feedback = completion.choices[0].message.content.strip()
    score = _extract_score(feedback)
    return feedback, score


def _clarify_question(document_text: str, question: str, user_message: str) -> str:
    completion = _client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a friendly examiner. Clarify the current viva question using the document text. "
                    "Keep it brief and stay focused on what the question is asking."
                ),
            },
            {
                "role": "user",
                "content": f"Document:\n{document_text}\n\nCurrent question: {question}\nStudent request: {user_message}",
            },
        ],
    )
    return completion.choices[0].message.content.strip()


def start_viva_session(payload: Dict[str, Any]) -> Dict[str, Any]:
    student_id = payload.get("student_id")
    unit_code = payload.get("unit_code")
    session_year = payload.get("session_year")
    assignment = payload.get("assignment")

    if not all([student_id, unit_code, session_year, assignment]):
        raise ValueError(
            "Missing required parameters: student_id, unit_code, session_year, assignment."
        )

    questions_response = generate_questions_logic(
        student_id=student_id,
        unit_code=unit_code,
        session=session_year,
        assignment=assignment,
    )
    questions = questions_response.get("questions", [])
    if len(questions) < 3:
        raise ValueError("Could not generate three questions from the document.")

    assignment_doc = get_student_assignment(
        student_id=student_id,
        unit_code=unit_code,
        session_year=session_year,
        assignment=assignment,
    )
    assignment_text = assignment_doc.get("content", "") if assignment_doc else ""
    if not assignment_text:
        raise ValueError("No assignment content found for this student.")

    guidance_text = _load_guidance_text()
    combined_text = (assignment_text + "\n\n" + guidance_text).strip()

    session_id = uuid.uuid4().hex
    _SESSIONS[session_id] = {
        "document_text": combined_text,
        "questions": questions,
        "answers": [],
        "next_index": 0,
    }

    return {
        "session_id": session_id,
        "question": questions[0],
        "question_number": 1,
        "total_questions": 3,
    }


def handle_viva_message(payload: Dict[str, Any]) -> Dict[str, Any]:
    session_id = payload.get("session_id")
    user_message = payload.get("user_message")
    intent = payload.get("intent", "answer")

    if not session_id or not user_message:
        raise ValueError("Missing required parameters: session_id, user_message.")

    session = _SESSIONS.get(session_id)
    if not session:
        raise KeyError("Session not found.")

    questions: List[str] = session["questions"]
    answers: List[str] = session["answers"]
    next_index: int = session["next_index"]
    document_text: str = session["document_text"]

    if next_index >= len(questions):
        return {
            "done": True,
            "feedback": session.get("feedback"),
            "score": session.get("score"),
        }

    if intent == "clarification":
        clarification = _clarify_question(
            document_text=document_text,
            question=questions[next_index],
            user_message=user_message,
        )
        return {
            "done": False,
            "message": clarification,
            "question_number": next_index + 1,
            "question": questions[next_index],
            "total_questions": len(questions),
        }

    answers.append(user_message.strip())
    session["next_index"] = next_index + 1

    if session["next_index"] >= len(questions):
        feedback, score = _grade_answers(document_text, questions, answers)
        session["feedback"] = feedback
        session["score"] = score
        return {
            "done": True,
            "feedback": feedback,
            "score": score,
            "total_questions": len(questions),
        }

    return {
        "done": False,
        "question": questions[session["next_index"]],
        "question_number": session["next_index"] + 1,
        "total_questions": len(questions),
    }
