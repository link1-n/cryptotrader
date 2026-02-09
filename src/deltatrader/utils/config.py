"""Configuration management."""

import os
from typing import Literal, cast

from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class Config:
    """Configuration for Delta Exchange trading."""

    # Environment: ['live', 'testnet']
    ENVIRONMENT: str = os.getenv("DELTA_ENVIRONMENT", "testnet")

    # Order Destination: ['paper', 'exchange']
    ORDER_DESTINATION: str = os.getenv("ORDER_DESTINATION", "paper")

    # API credentials
    API_KEY: str = os.getenv("DELTA_API_KEY", "")
    API_SECRET: str = os.getenv("DELTA_API_SECRET", "")

    # Logging
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    # Orderbook channel: ['l2_orderbook', 'l2_updates']
    # l2_orderbook: Full L2 snapshots sent periodically (max 20 symbols per connection)
    # l2_updates: Initial snapshot + incremental updates (max 100 symbols per connection)
    ORDERBOOK_CHANNEL: str = os.getenv("ORDERBOOK_CHANNEL", "l2_orderbook")

    # WebSocket URLs
    WS_PRODUCTION_URL = "wss://socket.india.delta.exchange"
    WS_TESTNET_URL = "wss://socket-ind.testnet.deltaex.org"

    # REST API URLs
    REST_PRODUCTION_URL = "https://api.india.delta.exchange"
    REST_TESTNET_URL = "https://cdn-ind.testnet.deltaex.org"

    # Connection settings
    WS_HEARTBEAT_INTERVAL = 30  # seconds
    WS_RECONNECT_DELAY = 5  # seconds
    WS_MAX_RECONNECT_ATTEMPTS = 10
    REST_TIMEOUT = 10  # seconds

    @classmethod
    def get_ws_url(cls) -> str:
        """Get WebSocket URL based on environment."""
        return (
            cls.WS_TESTNET_URL
            if cls.ENVIRONMENT == "testnet"
            else cls.WS_PRODUCTION_URL
        )

    @classmethod
    def get_rest_url(cls) -> str:
        """Get REST API URL based on environment."""
        return (
            cls.REST_TESTNET_URL
            if cls.ENVIRONMENT == "testnet"
            else cls.REST_PRODUCTION_URL
        )

    @classmethod
    def is_demo_mode(cls) -> bool:
        """Check if running in demo/test mode."""
        return cls.ENVIRONMENT == "testnet"

    @classmethod
    def validate(cls) -> bool:
        """Validate configuration."""
        if not cls.API_KEY or not cls.API_SECRET:
            return False
        return True
