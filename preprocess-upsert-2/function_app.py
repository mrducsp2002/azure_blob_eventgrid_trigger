import azure.functions as func
import logging
import os
import io
import zipfile
from azure.storage.blob import BlobServiceClient
from PyPDF2 import PdfReader, PdfMerger
from reportlab.lib.pagesizes import letter
from reportlab.platypus import Preformatted, SimpleDocTemplate
from reportlab.lib.styles import getSampleStyleSheet
from openai import OpenAI
from pymongo import MongoClient, ReplaceOne

app = func.FunctionApp()

# --- GLOBAL CLIENTS (Connection Pooling) ---
# Initialize these once so they are reused across function invocations
openai_client = None
mongo_collection = None


def get_openai_client():
    global openai_client
    if openai_client is None:
        openai_client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
    return openai_client


def get_mongo_collection():
    global mongo_collection
    if mongo_collection is None:
        mongo_str = os.environ["MONGO_CONNECTION_STRING"]
        db_name = os.environ["DB_NAME"]
        col_name = os.environ["COLLECTION_NAME"]
        client = MongoClient(mongo_str)
        mongo_collection = client[db_name][col_name]
    return mongo_collection


# --- HELPER FUNCTIONS ---
def _text_to_pdf(text_content):
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter)
    style = getSampleStyleSheet()
    story = [Preformatted(text_content, style['Code'])]
    doc.build(story)
    buffer.seek(0)
    return buffer


def _chunk_text(text, chunk_size=500, overlap=100):
    words = text.split()
    chunks = []
    start = 0
    while start < len(words):
        end = start + chunk_size
        chunk = ' '.join(words[start:end])
        chunks.append(chunk)
        start += chunk_size - overlap
    return chunks


# --- FUNCTION 1: UNZIP & MERGE ---
@app.blob_trigger(arg_name="myblob", path="incoming-zips/{name}", connection="AzureWebJobsStorage")
def unzip_and_merge(myblob: func.InputStream):
    logging.info(f"Processing ZIP: {myblob.name}")

    try:
        # Setup Blob Client using the correct connection string
        connect_str = os.environ["AzureWebJobsStorage"]
        blob_service_client = BlobServiceClient.from_connection_string(
            connect_str)
        container_client = blob_service_client.get_container_client("raw-pdfs")
        if not container_client.exists():
            container_client.create_container()

        # Read Zip into Memory
        zip_bytes = io.BytesIO(myblob.read())

        with zipfile.ZipFile(zip_bytes) as z:
            all_files = z.namelist()
            student_groups = {}

            # Group Logic
            for filepath in all_files:
                if "__MACOSX" in filepath or filepath.endswith("/") or ".DS_Store" in filepath:
                    continue
                parts = filepath.split("/")
                key = parts[0]
                if key not in student_groups:
                    student_groups[key] = []
                student_groups[key].append(filepath)

            logging.info(f"Found {len(student_groups)} unique groups.")

            # Processing files
            for student_key, files in student_groups.items():
                output_filename = f"{student_key}.pdf" if not student_key.endswith(
                    ".pdf") else student_key
                files.sort()

                try:
                    output_stream = io.BytesIO()
                    merger = PdfMerger()
                    has_content = False

                    for file_in_zip in files:
                        # We open the file from the zip archive here
                        with z.open(file_in_zip) as f:
                            file_bytes = f.read()
                            file_data = io.BytesIO(file_bytes)

                            if not file_in_zip.lower().endswith(".pdf"):
                                # Decode text safely
                                text_content = file_bytes.decode(
                                    'utf-8', errors='ignore')
                                pdf_data = _text_to_pdf(text_content)
                                merger.append(pdf_data)
                                has_content = True
                            else:
                                merger.append(file_data)
                                has_content = True

                    if has_content:
                        merger.write(output_stream)
                        merger.close()
                        output_stream.seek(0)

                        blob_client = container_client.get_blob_client(
                            output_filename)
                        blob_client.upload_blob(output_stream, overwrite=True)
                        logging.info(f"Uploaded: {output_filename}")

                except Exception as e:
                    logging.error(
                        f"Error processing student {student_key}: {e}")

    except Exception as e:
        logging.error(f"Error processing zip {myblob.name}: {e}")


# --- FUNCTION 2: UPSERT TO MONGODB ---
@app.blob_trigger(arg_name="myblob", path="raw-pdfs/{name}", connection="AzureWebJobsStorage")
def blob_process_upsert(myblob: func.InputStream):
    logging.info(f"Upserting PDF: {myblob.name}")

    try:
        # Use Global Clients
        client_openai = get_openai_client()
        collection = get_mongo_collection()

        # Read PDF
        pdf_stream = io.BytesIO(myblob.read())
        reader = PdfReader(pdf_stream)

        text = ""
        for page in reader.pages:
            extract = page.extract_text()
            if extract:
                text += extract

        if not text:
            logging.warning(f"No text found in {myblob.name}")
            return

        chunks = _chunk_text(text)
        filename = os.path.basename(myblob.name)

        mongo_documents = []

        for i, chunk in enumerate(chunks):
            try:
                resp = client_openai.embeddings.create(
                    model='text-embedding-3-large',
                    input=chunk,
                    dimensions=1024
                )

                doc = {
                    "_id": f"{filename}_chunk_{i}",
                    "contentVector": resp.data[0].embedding,
                    "text": chunk,
                    "filename": filename,
                    "chunk_index": i
                }
                mongo_documents.append(doc)
            except Exception as e:
                logging.error(f"Error embedding chunk {i} of {filename}: {e}")

        if mongo_documents:
            requests = [ReplaceOne({"_id": d["_id"]}, d, upsert=True)
                        for d in mongo_documents]
            collection.bulk_write(requests)
            logging.info(
                f"DONE! Upserted {len(mongo_documents)} chunks for {filename}.")

    except Exception as e:
        logging.error(f"Error processing blob {myblob.name}: {e}")
