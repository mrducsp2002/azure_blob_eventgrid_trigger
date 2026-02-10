import logging
import json
import os
import re
from openai import AzureOpenAI
from src.database import get_student_assignment, get_staff_document

# Initialize OpenAI 
client = AzureOpenAI(
    api_version="2024-12-01-preview",
    azure_endpoint="https://iviva.cognitiveservices.azure.com/",
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
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
        - **Seed Alignment:** Use the provided seed questions as inspiration.
        - **Quantity:** Generate exactly the same number of questions as provided in the seed input.
        - **Tone:** Use intermediate spoken language suitable for a verbal assessment.

        # Steps
        1. Analyze the seed questions for style and scope.
        2. Review the assignment content to find relevant arguments or data.
        3. Generate questions that explicitly reference the student's work.
        4. Output strictly in the JSON format below.

        # Output Format
        Return ONLY raw JSON.

        {{
        "student_id": "{student_id}",
        "questions": ["Question 1", "Question 2"],
        "reference": ["Reference for Q1", "Reference for Q2"]
        }}
        """

    response = client.chat.completions.create(
        model="gpt-4o",
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"""
                CONTEXT:
                Assessment brief: {brief_text}
                Assessment rubric: {rubric_text}
                Assignment: {resolved_assignment_text}
                Seed questions:
                {format_seed_questions(seed_questions)}
            """}
        ]
    )

    # Return the parsed JSON object directly
    return json.loads(response.choices[0].message.content)


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

    response = client.chat.completions.create(
        model="gpt-4o",
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"""
                CURRENT QUESTION:
                {current_question}

                USER COMMENT:
                {user_comment}
            """}
        ]
    )

    # Return the parsed JSON object directly
    return json.loads(response.choices[0].message.content)
