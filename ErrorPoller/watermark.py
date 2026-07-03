"""
ErrorPoller/watermark.py
─────────────────────────
Stores watermarks for BOTH polling sources as a single JSON blob.

Container: aiops-watermarks
Blob:      finance-error-poller-watermark.json
Content:   { "runlog": <int>, "apperrors": <int> }

Both keys always present. Missing keys default to 0 (first run).
"""

import json
import logging
import os

from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient

logger = logging.getLogger(__name__)

CONTAINER = "aiops-watermarks"
BLOB_NAME = "finance-error-poller-watermark.json"

_DEFAULT = {"runlog": 0, "apperrors": 0}


def get_watermark() -> dict:
    """
    Returns { "runlog": int, "apperrors": int }.
    Returns { "runlog": 0, "apperrors": 0 } on first run or read failure.
    """
    try:
        client  = _get_blob_client()
        content = client.download_blob().readall().decode("utf-8").strip()
        data    = json.loads(content)

        # Tolerate old single-int watermark from a previous deployment
        if isinstance(data, int):
            logger.info("Migrating legacy integer watermark %d to dual-key format", data)
            return {"runlog": data, "apperrors": 0}

        return {
            "runlog":    int(data.get("runlog",    0)),
            "apperrors": int(data.get("apperrors", 0)),
        }
    except ResourceNotFoundError:
        logger.info("Watermark blob not found — first run, returning defaults")
        return dict(_DEFAULT)
    except Exception as e:
        logger.warning("Watermark read failed (%s) — defaulting to %s", e, _DEFAULT)
        return dict(_DEFAULT)


def update_watermark(new_values: dict) -> None:
    """
    Merge new_values into the existing watermark and persist.
    Only advances values (never goes backwards).

    new_values example: { "runlog": 42 }   — only updates runlog key
    """
    try:
        current = get_watermark()
        for key, val in new_values.items():
            if val > current.get(key, 0):
                current[key] = val

        _ensure_container()
        client = _get_blob_client()
        client.upload_blob(json.dumps(current).encode("utf-8"), overwrite=True)
        logger.info("Watermark updated: %s", current)
    except Exception as e:
        logger.error("Watermark update failed: %s", e)


def _get_blob_client():
    svc = BlobServiceClient.from_connection_string(os.environ["STORAGE_CONNECTION_STRING"])
    return svc.get_blob_client(container=CONTAINER, blob=BLOB_NAME)


def _ensure_container():
    try:
        svc = BlobServiceClient.from_connection_string(os.environ["STORAGE_CONNECTION_STRING"])
        c   = svc.get_container_client(CONTAINER)
        if not c.exists():
            c.create_container()
            logger.info("Created container: %s", CONTAINER)
    except Exception as e:
        logger.warning("Container check failed: %s", e)