"""Authentication and signing utilities for Delta Exchange API."""

import hashlib
import hmac

from ..utils.config import Config
from ..utils.timing import get_timestamp_seconds


def sign_request(
    method: str,
    path: str,
    query_string: str = "",
    body: str = "",
    timestamp: int | None = None,
) -> tuple[str, int]:
    """
    Sign a REST API request using HMAC-SHA256.

    Args:
        method: HTTP method (GET, POST, PUT, DELETE)
        path: Request path (e.g., "/v2/orders")
        query_string: Query parameters as string (e.g., "product_id=123")
        body: Request body as JSON string
        timestamp: Unix timestamp in seconds (auto-generated if None)

    Returns:
        Tuple of (signature, timestamp)
    """
    if timestamp is None:
        timestamp = get_timestamp_seconds()

    # Build signature payload: method + timestamp + path + query + body
    # Example: "GET1234567890/v2/productspage_size=100"
    signature_data = method + str(timestamp) + path

    if query_string:
        signature_data += query_string

    if body:
        signature_data += body

    # Generate HMAC-SHA256 signature
    signature = hmac.new(
        Config.API_SECRET.encode("utf-8"),
        signature_data.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    return signature, timestamp


def sign_websocket_auth(timestamp: int | None = None) -> tuple[str, int]:
    """
    Sign WebSocket authentication message.

    WebSocket auth signature = HMAC-SHA256 of 'GET' + timestamp + '/live'

    Args:
        timestamp: Unix timestamp in seconds (auto-generated if None)

    Returns:
        Tuple of (signature, timestamp)
    """
    if timestamp is None:
        timestamp = get_timestamp_seconds()

    # Build signature payload: GET + timestamp + /live
    signature_data = "GET" + str(timestamp) + "/live"

    # Generate HMAC-SHA256 signature
    signature = hmac.new(
        Config.API_SECRET.encode("utf-8"),
        signature_data.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    return signature, timestamp


def get_auth_headers(signature: str, timestamp: int) -> dict[str, str]:
    """
    Get authentication headers for REST API requests.

    Args:
        signature: HMAC-SHA256 signature
        timestamp: Unix timestamp in seconds

    Returns:
        Dictionary of headers
    """
    return {
        "api-key": Config.API_KEY,
        "signature": signature,
        "timestamp": str(timestamp),
        "User-Agent": "crypt/0.1.0",
        "Content-Type": "application/json",
    }


def create_websocket_auth_message(signature: str, timestamp: int) -> dict:
    """
    Create WebSocket authentication message.

    Args:
        signature: HMAC-SHA256 signature
        timestamp: Unix timestamp in seconds

    Returns:
        WebSocket auth message dict
    """
    return {
        "type": "auth",
        "payload": {
            "api-key": Config.API_KEY,
            "signature": signature,
            "timestamp": str(timestamp),
        },
    }
