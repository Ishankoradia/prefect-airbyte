import httpx
import os
from typing import Dict, Any

from prefect.utilities.asyncutils import sync_compatible


def sort_dict(d: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively sort a dictionary by its keys."""
    return {
        k: (
            sort_dict(v)
            if isinstance(v, dict)
            else sorted(v) if isinstance(v, list) else v
        )
        for k, v in sorted(d.items())
    }


@sync_compatible
async def post_dalgo_airbyte_webhook(job_id: int):
    """Post a webhook to the Dalgo API for an airbyte job."""

    url = f"{os.getenv('DALGO_BACKEND_BASE_URI')}/webhooks/v1/airbyte_job/{job_id}"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-Notification-Key": os.getenv("AIRBYTE_NOTIFICATIONS_WEBHOOK_KEY", ""),
    }
    payload = {}

    # Use httpx.AsyncClient for async HTTP requests
    async with httpx.AsyncClient() as client:
        response = await client.post(
            url,
            headers=headers,
            json=payload,  # httpx automatically serializes to JSON
            timeout=15.0,  # 15 second timeout
        )
        # This raises an HTTPStatusError if the response status is 4XX/5XX
        response.raise_for_status()
        result = response.json()
        print(f"Webhook successfully sent for job_id: {job_id}")
        return result
