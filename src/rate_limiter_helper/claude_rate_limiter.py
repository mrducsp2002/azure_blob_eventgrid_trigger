import json
import os
from datetime import datetime, timedelta, timezone

from azure.core.exceptions import HttpResponseError, ResourceExistsError, ResourceNotFoundError
from azure.data.tables import TableServiceClient, UpdateMode
from azure.servicebus import ServiceBusClient, ServiceBusMessage

_DEFAULT_TOKENS_PER_MIN = "350000"
_DEFAULT_TABLE_NAME = "claude-token-bucket"
_DEFAULT_BUCKET_KEY = "claude"

_TOKEN_TABLE_CLIENT = None


def get_tokens_per_min() -> int:
    return max(1, int(os.environ.get("CLAUDE_TOKENS_PER_MIN", _DEFAULT_TOKENS_PER_MIN)))


def _get_bucket_key() -> str:
    return os.environ.get("CLAUDE_RATE_LIMIT_KEY", _DEFAULT_BUCKET_KEY)


def _get_bucket_table_name() -> str:
    return os.environ.get("TOKEN_BUCKET_TABLE_NAME", _DEFAULT_TABLE_NAME)


def _get_table_connection() -> str:
    return os.environ.get("TOKEN_BUCKET_TABLE_CONNECTION") or os.environ.get("AzureWebJobsStorage", "")


def _get_token_table_client():
    global _TOKEN_TABLE_CLIENT
    if _TOKEN_TABLE_CLIENT is not None:
        return _TOKEN_TABLE_CLIENT
    connection = _get_table_connection()
    if not connection:
        raise ValueError("TOKEN_BUCKET_TABLE_CONNECTION or AzureWebJobsStorage is not configured.")
    service = TableServiceClient.from_connection_string(connection)
    table_name = _get_bucket_table_name()
    try:
        service.create_table(table_name)
    except ResourceExistsError:
        pass
    _TOKEN_TABLE_CLIENT = service.get_table_client(table_name)
    return _TOKEN_TABLE_CLIENT


def _current_minute_bucket():
    now = datetime.now(timezone.utc)
    minute_start = now.replace(second=0, microsecond=0)
    return now, minute_start, str(int(minute_start.timestamp()))


def try_consume_tokens(estimated_tokens: int):
    table_client = _get_token_table_client()
    tokens_per_min = get_tokens_per_min()
    bucket_key = _get_bucket_key()
    for _ in range(5):
        now, minute_start, row_key = _current_minute_bucket()
        try:
            entity = table_client.get_entity(partition_key=bucket_key, row_key=row_key)
        except ResourceNotFoundError:
            entity = {
                "PartitionKey": bucket_key,
                "RowKey": row_key,
                "used_tokens": 0,
                "limit": tokens_per_min,
            }
            try:
                table_client.create_entity(entity)
            except ResourceExistsError:
                pass
            entity = table_client.get_entity(partition_key=bucket_key, row_key=row_key)

        used_tokens = int(entity.get("used_tokens", 0))
        if used_tokens + estimated_tokens > tokens_per_min:
            remaining = max(0, tokens_per_min - used_tokens)
            reset_in = max(1, int(60 - (now - minute_start).total_seconds()))
            return False, remaining, reset_in

        entity["used_tokens"] = used_tokens + estimated_tokens
        entity["limit"] = tokens_per_min
        entity["updated_at"] = now.isoformat()
        try:
            table_client.update_entity(entity, mode=UpdateMode.REPLACE, etag=entity.get("etag"))
            remaining = max(0, tokens_per_min - entity["used_tokens"])
            reset_in = max(1, int(60 - (now - minute_start).total_seconds()))
            return True, remaining, reset_in
        except HttpResponseError as exc:
            if exc.status_code in (409, 412):
                continue
            raise

    return False, 0, 5


def reschedule_service_bus_message(
    payload: dict,
    delay_seconds: int,
    queue_name: str,
    connection_env: str = "SERVICEBUS_CONNECTION",
) -> None:
    connection_string = os.environ.get(connection_env)
    if not connection_string:
        raise ValueError(f"{connection_env} is not configured.")
    scheduled_time = datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)
    with ServiceBusClient.from_connection_string(connection_string) as client:
        with client.get_queue_sender(queue_name) as sender:
            message = ServiceBusMessage(json.dumps(payload))
            sender.schedule_messages(message, scheduled_time)
