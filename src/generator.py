import logging
import json
import os
from openai import AzureOpenAI
from src.database import get_student_assignment

# Initialize OpenAI 
client = AzureOpenAI(
    api_version="2024-12-01-preview",
    azure_endpoint="https://iviva.cognitiveservices.azure.com/",
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
)

def generate_questions_logic(student_id, unit_code, session, assignment):
    """
    Orchestrates the data fetching and AI generation.
    Returns: A dictionary (JSON) of questions or Raises an Exception.
    """
    # 1. Fetch Document
    docs = {
        "Student Assignment": get_student_assignment(
            student_id=student_id,
            unit_code=unit_code,
            session_year=session,
            assignment=assignment,
        )
        # "Assessment Brief": db["iviva-staff-assessment-brief"].find_one({
        #     "unit_code": unit_code,
        #     "session_year": session,
        #     "assignment": assignment
        # }),
        # "Rubric": db["iviva-staff-assessment-rubrics"].find_one({
        #     "unit_code": unit_code,
        #     "session_year": session,
        #     "assignment": assignment
        # }),
        # "Seed Questions": db["iviva-staff-seed-questions"].find_one({
        #     "unit_code": unit_code,
        #     "session_year": session,
        #     "assignment": assignment
        # })
    }

    # # Check if all documents are present
    # missing = [name for name, doc in docs.items() if not doc]

    # if missing:
    #     raise ValueError(f"Missing required documents: {', '.join(missing)}")

    # # 3. Extract Content
    assignment_text = docs["Student Assignment"].get("content", "")
    # brief_text = docs["Assessment Brief"].get("content", "")
    # rubric_text = docs["Rubric"].get("content", "")
    # seed_text = docs["Seed Questions"].get("content", "")

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
                Assignment: {assignment_text}
                Seed questions: 
                Topic 1: Game Logic and Mechanics
                Topic 2: Use of Variables and Randomisation
                Topic 3: User Interaction and Controls
            """}
        ]
    )

    # Return the parsed JSON object directly
    return json.loads(response.choices[0].message.content)

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
