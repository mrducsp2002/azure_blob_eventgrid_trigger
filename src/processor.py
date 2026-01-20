import io
import os
import logging
import zipfile
from collections import defaultdict
from src.parsers import decode_file_content
from src.database import store_document

# TODO: Change approach to extrach metadata
# Easier approach to extract metadata (for now). If necessary, pull the metadata being sent from the front end (what the conveyor has filled in instead of relying on naming convention)
def extract_batch_metadata(blob_name: str) -> dict:
    """Extracts metadata from filename format: Unit_Ass_Year"""
    clean_name = os.path.splitext(os.path.basename(blob_name))[0]
    parts = clean_name.split('_')

    if len(parts) < 3:
        logging.warning(
            f"Blob name '{blob_name}' invalid format. Using defaults.")
        return {
            'unit_code': 'UNKNOWN_UNIT',
            'assignment': 'UNKNOWN_ASS',
            'session_year': 'UNKNOWN_YEAR'
        }

    return {
        'unit_code': parts[0],
        'assignment': parts[1],
        'session_year': parts[2]
    }
    
def process_blob_stream(content_bytes: bytes, blob_name: str, collection):
    if blob_name.endswith('.zip'):
        _process_zip_file(content_bytes, blob_name, collection)
    else:
        _process_single_file(content_bytes, blob_name, collection)


# --- 3. INTERNAL HANDLERS ---
def _process_zip_file(zip_bytes: bytes, blob_name: str, collection):
    """
    Handle ZIP files
    """
    batch_metadata = extract_batch_metadata(blob_name)
    zip_buffer = io.BytesIO(zip_bytes)
    student_buffers = defaultdict(list)

    with zipfile.ZipFile(zip_buffer, 'r') as zip_ref:
        file_list = zip_ref.namelist()

        for file_path in file_list:
            if file_path.endswith("/") or "__MACOSX" in file_path or ".DS_Store" in file_path:
                continue

            # ZIP Logic: Expects "StudentID/filename.ext"
            parts = file_path.split('/')
            if len(parts) < 2:
                continue

            student_id = parts[1].split('-')[0]  # Extract student ID
            file_name = parts[-1]

            try:
                with zip_ref.open(file_path) as file:
                    content_bytes = file.read()
                    text_chunk = decode_file_content(file_name, content_bytes)
                    if text_chunk:
                        formatted_chunk = f"\n\n--- START FILE: {file_name} ---\n{text_chunk}"
                        student_buffers[student_id].append(formatted_chunk)
            except Exception as e:
                logging.error(f"Error reading {file_path}: {e}")
                continue

    # Save Student Data
    for student_id, text_parts in student_buffers.items():
        if not text_parts:
            continue
        full_merged_text = "".join(text_parts)

        # Add Student ID to metadata
        final_metadata = batch_metadata.copy()
        final_metadata['student_id'] = student_id

        store_document(collection, final_metadata, full_merged_text, blob_name)


def _process_single_file(file_bytes: bytes, blob_name: str, collection):
    """
    Handles single PDFs/DOCX (Staff Briefs & Rubrics).
    """
    # 1. Extract Metadata
    metadata = extract_batch_metadata(blob_name)

    # 2. Decode Content
    text_content = decode_file_content(blob_name, file_bytes)

    if not text_content:
        logging.warning(f"File {blob_name} was empty or could not be decoded.")
        return

    # 3. Store
    store_document(collection, metadata, text_content, blob_name)
