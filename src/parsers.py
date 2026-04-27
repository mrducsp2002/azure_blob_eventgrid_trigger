import io
import os
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential
import logging

ENDPOINT = os.environ.get("DOCUMENT_INTELLIGENCE_ENDPOINT")
KEY = os.environ.get("DOCUMENT_INTELLIGENCE_KEY")


def decode_file_content(file_name: str, content: bytes) -> str:
    """Decodes file bytes using Azure AI Document Intelligence for PDFs."""
    file_ext = os.path.splitext(file_name)[1].lower().strip('.')
    text_result = ""

    azure_supported_exts = ['pdf', 'docx', 'doc', 'md']

    try:
        if file_ext in azure_supported_exts:
            client = DocumentAnalysisClient(
                endpoint=ENDPOINT,
                credential=AzureKeyCredential(KEY)
            )

            poller = client.begin_analyze_document(
                "prebuilt-read", document=io.BytesIO(content))
            result = poller.result()

            text_result = result.content

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
