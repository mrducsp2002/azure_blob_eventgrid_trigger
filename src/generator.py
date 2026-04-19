import logging
import json
import os
import re
from anthropic import AnthropicFoundry
from src.database import get_student_assignment, get_staff_document
from typing import Any, Dict, List, Optional, Tuple

# Initialize Anthropic Claude
client = AnthropicFoundry(
    api_key=os.getenv("CLAUDE_API_KEY"),
    base_url=os.getenv("CLAUDE_ENDPOINT"),
)

def generate_questions_logic(student_id, unit_code, session, assignment=None, assignment_text=None):
    """
    Orchestrates the data fetching and AI generation.
    Returns: A dictionary (JSON) of questions or Raises an Exception.
    """
    # 1. Fetch Document
    docs = {
        "Assessment Brief": get_staff_document(
            collection_name="iviva-staff-assessment-brief",
            unit_code=unit_code,
            session_year=session,
            assignment=assignment,
        ),
        "Assessment Rubric": get_staff_document(
            collection_name="iviva-staff-assessment-rubrics",
            unit_code=unit_code,
            session_year=session,
            assignment=assignment,
        ),
        "Seed Questions": get_staff_document(
            collection_name="iviva-staff-seed-questions",
            unit_code=unit_code,
            session_year=session,
            assignment=assignment,
        ),
    }

    # Resolve assignment text from uploaded content first, then DB lookup.
    resolved_assignment_text = (assignment_text or "").strip()
    if not resolved_assignment_text:
        if not assignment:
            raise ValueError(
                "Missing assignment content. Provide uploaded assignment text or assignment metadata."
            )
        student_doc = get_student_assignment(
            student_id=student_id,
            unit_code=unit_code,
            session_year=session,
            assignment=assignment,
        )
        if student_doc:
            resolved_assignment_text = (student_doc.get("content", "") or "").strip()

    if not resolved_assignment_text:
        raise ValueError("No assignment content found for this student.")

    # 2. Check required guidance documents
    missing = [name for name, doc in docs.items() if not doc]

    if missing:
        raise ValueError(f"Missing required documents: {', '.join(missing)}")

    # 3. Extract Content
    brief_text = docs["Assessment Brief"].get("content", "")
    rubric_text = docs["Assessment Rubric"].get("content", "")
    seed_text = docs["Seed Questions"].get("content", "")

    seed_questions = _parse_seed_questions(seed_text)
    if not seed_questions:
        raise ValueError("Seed questions document is empty.")

    # 3. Call AI
    system_prompt = f"""
        You are an expert academic examiner. Generate tailored Viva Voce questions for a student based on their specific assignment content.

        # Guidelines
        - **Context-Specific:** Ensure each question relates directly to the student's submitted assignment.
        - **Quantity:** Generate exactly the same number of questions as provided in the seed questions. If there are n questions in the seed questions pack, generate exactly n questions for the students, no more, no less.
        - **Seed Alignment:** Use the provided seed questions as inspiration.
        - **Tone:** Use intermediate spoken language suitable for a verbal assessment.

        # Steps
        1. Analyze the seed questions for style and scope.
        2. Review the assignment content to find relevant arguments or data.
        3. Generate questions that explicitly reference the student's work, the number of questions must match the number of seed questions provided.
        4. Output strictly in the JSON format below.

        # Output Format
        Return ONLY raw JSON.

        {{
        "student_id": "{student_id}",
        "questions": ["Question 1", "Question 2"],
        "reference": ["Reference for Q1", "Reference for Q2"]
        }}
        """

    # Build user message for request
    user_message_content = f"""
                CONTEXT:
                Assessment brief: {brief_text}
                Assessment rubric: {rubric_text}
                Assignment: {resolved_assignment_text}
                Seed questions:
                {format_seed_questions(seed_questions)}

                Now generate {len(seed_questions)} personalized questions given the context above and follow the seed questions structure.
            """

    # Log Claude request metadata (production-safe)
    logging.info(
        "Claude request - student_id=%s model=claude-sonnet-4-6 system_prompt_chars=%d user_message_chars=%d seed_count=%d",
        student_id,
        len(system_prompt),
        len(user_message_content),
        len(seed_questions),
    )

    # Debug: log full messages if environment variable enabled
    if os.getenv("DEBUG_CLAUDE_MESSAGES", "false").lower() == "true":
        logging.debug(
            "Claude debug - system_prompt: %s..., user_message: %s...",
            system_prompt[:500],
            user_message_content[:500],
        )

    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2048,
        system=system_prompt,
        messages=[
            {"role": "user", "content": user_message_content}
        ],
    )

    # Return the parsed JSON object directly
    return _parse_json_response(response.content[0].text, ["student_id", "questions", "reference"])


def _parse_json_response(raw_text: str, required_keys: List[str]) -> Dict[str, Any]:
    """
    Parse model output into JSON and validate required keys.
    Handles common wrappers such as markdown code fences and extra prose.
    """
    text = (raw_text or "").strip()

    if text.startswith("```"):
        lines = text.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        text = "\n".join(lines).strip()

    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise ValueError("Model response did not contain a valid JSON object.")
        payload = json.loads(text[start : end + 1])

    if not isinstance(payload, dict):
        raise ValueError("Model response JSON must be an object.")

    missing_keys = [key for key in required_keys if key not in payload]
    if missing_keys:
        raise ValueError(f"Model response JSON missing keys: {', '.join(missing_keys)}")

    return payload


def _parse_seed_questions(seed_text: str) -> list:
    lines = [line.strip() for line in seed_text.splitlines()]
    questions = []
    for line in lines:
        if not line:
            continue
        cleaned = re.sub(r"^\s*\d+[\).\s-]+", "", line).strip()
        if cleaned:
            questions.append(cleaned)
    return questions


def format_seed_questions(seed_questions: list) -> str:
    return "\n".join([f"{idx + 1}. {question}" for idx, question in enumerate(seed_questions)])

def regenerate_questions_logic(current_question, user_comment):
    """
    Regenerates questions based on user feedback.
    Returns: A dictionary (JSON) of regenerated questions or Raises an Exception.
    """
    # 3. Call AI
    system_prompt = f"""
        You are an expert academic examiner. The user has read the question and make some comments to refine the questions.
        Regenerate the question based on user feedback.

        # Guidelines
        - **Incorporate Feedback:** Modify the current question based on the user's comment.
        - **Clarity and Relevance:** Ensure the regenerated question is clear and relevant to the assignment context.
        - **Tone:** Use intermediate spoken language suitable for a verbal assessment.

        # Output Format
        Return ONLY raw JSON.

        {{
        "regenerated_question": "Regenerated Question",
        "explanation": "Explanation of changes made"
        }}
        """

    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2048,
        system=system_prompt,
        messages=[
            {"role": "user", "content": f"""
                CURRENT QUESTION:
                {current_question}

                USER COMMENT:
                {user_comment}
            """}
        ],
    )

    # Return the parsed JSON object directly
    return _parse_json_response(response.content[0].text, ["regenerated_question", "explanation"])

def generate_feedback(unit_code: str, session: str, assignment:str, questions: List[str], answers: List[str]) -> str:
    """
    Generates feedback based on the student's answers to the questions.
    Returns: Feedback as string 
    """
    
    # 1. Fetch Document
    docs = {
        "Assessment Brief": get_staff_document(
            collection_name="iviva-staff-assessment-brief",
            unit_code=unit_code,
            session_year=session,
            assignment=assignment,
        ),
        "Assessment Rubric": get_staff_document(
            collection_name="iviva-staff-assessment-rubrics",
            unit_code=unit_code,
            session_year=session,
            assignment=assignment,
        )}
    
    missing = [name for name, docs in docs.items() if not docs]
    
    if missing: 
        raise ValueError(f"Missing required documents for feedback generation: {', '.join(missing)}")
    
    # 2. Extract Content
    brief_text = docs["Assessment Brief"].get("content", "")
    rubric_text = docs["Assessment Rubric"].get("content", "")
    
    # 3. Call AI
    feedback_prompt = """
        "You are an examiner grading viva answers using only the provided context from the assessment brief and rubric."
        "Given the viva questions and the student's answers, provide concise feedback grounded in the text. "
        "If the text does not support an answer, call that out. "
        "Output:\n\n"
        "### FEEDBACK\n"
        "- Summary: <one short sentence>\n"
        "- Per question: Q1 <feedback>; Q2 <feedback>; Q3 <feedback>\n\n"
        "### SOURCES\n"
        "<one source per line, with text from the retrieved chunk sources and the specific line they come from>\n"
    """
    
    qa_text = "\n".join(
        [f"Q{i + 1}: {questions[i]}\nA{i + 1}: {answers[i]}" for i in range(
            len(questions))]
    )
    
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2048,
        system=feedback_prompt,
        messages=[
            {"role": "user", "content": f"CONTEXT: \n Assessment Brief: {brief_text} \n Assessment Rubric: {rubric_text} \n\n Questions and Answers:\n{qa_text}"},
        ],
    ).content[0].text.strip()
    return response

