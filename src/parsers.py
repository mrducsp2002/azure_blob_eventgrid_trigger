import io
import os
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential

ENDPOINT = os.environ.get("DOCUMENT_INTELLIGENCE_ENDPOINT")
KEY = os.environ.get("DOCUMENT_INTELLIGENCE_KEY")

def decode_file_content(file_name: str, content: bytes) -> str:
    """Decodes file bytes using Azure AI Document Intelligence for PDFs."""
    file_ext = os.path.splitext(file_name)[1].lower().strip('.')
    text_result = ""

    try:
        if file_ext == 'pdf':
            client = DocumentAnalysisClient(
                endpoint=ENDPOINT,
                credential=AzureKeyCredential(KEY)
            )
            
            poller = client.begin_analyze_document("prebuilt-read", document=io.BytesIO(content))
            result = poller.result()
            
            text_result = result.content

        # elif file_ext == 'docx':
        #     doc = Document(file_stream)
        #     full_text = []
        #     for para in doc.paragraphs:
        #         full_text.append(para.text)
        #     for table in doc.tables:
        #         for row in table.rows:
        #             row_text = " | ".join([cell.text for cell in row.cells])
        #             full_text.append(row_text)
        #     text_result = "\n".join(full_text)

        else:
            # Fallback for code files / text files
            try:
                text_result = content.decode('utf-8')
            except UnicodeDecodeError:
                text_result = content.decode('latin-1')

        return text_result.strip()

    except Exception as e:
        logging.error(f"Decoding error for {file_name}: {str(e)}")
        return ''
