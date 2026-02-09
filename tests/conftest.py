"""Pytest configuration and shared fixtures."""

import asyncio
import os
from typing import AsyncGenerator

import pytest

from deltatrader.client.rest import RestClient
from deltatrader.core.live_order_manager import LiveOrderManager
from deltatrader.core.paper_order_manager import PaperOrderManager
from deltatrader.models.product import Product
from deltatrader.utils.config import Config
from deltatrader.utils.integer_conversion import IntegerConverter


@pytest.fixture(scope="session")
def event_loop():
    """Create an event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def converter() -> IntegerConverter:
    """Create an IntegerConverter instance."""
    return IntegerConverter()


@pytest.fixture
def test_product() -> Product:
    """Create a test product."""
    return Product(
        product_id=84,
        symbol="BTCUSD",
        description="Bitcoin Perpetual Futures",
        contract_type="perpetual_futures",
        tick_size="0.5",
        contract_size="1",
        quoting_asset="USD",
        settling_asset="USDT",
    )


@pytest.fixture
def registered_converter(
    converter: IntegerConverter, test_product: Product
) -> IntegerConverter:
    """Create an IntegerConverter with a registered product."""
    converter.register_product(test_product)
    return converter


@pytest.fixture
async def rest_client() -> AsyncGenerator[RestClient, None]:
    """Create a REST client for testing."""
    client = RestClient()
    await client.connect()
    yield client
    await client.close()


@pytest.fixture
async def testnet_rest_client() -> AsyncGenerator[RestClient, None]:
    """Create a REST client connected to testnet."""
    # Save original config
    original_env = Config.ENVIRONMENT

    # Force testnet
    Config.ENVIRONMENT = "testnet"

    client = RestClient()
    await client.connect()

    yield client

    await client.close()

    # Restore original config
    Config.ENVIRONMENT = original_env


@pytest.fixture
def paper_order_manager(registered_converter: IntegerConverter) -> PaperOrderManager:
    """Create a PaperOrderManager instance."""
    return PaperOrderManager(registered_converter)


@pytest.fixture
async def live_order_manager(
    testnet_rest_client: RestClient, registered_converter: IntegerConverter
) -> LiveOrderManager:
    """Create a LiveOrderManager instance connected to testnet."""
    manager = LiveOrderManager(testnet_rest_client, registered_converter)
    return manager


@pytest.fixture
def skip_if_no_credentials():
    """Skip test if API credentials are not available."""
    if not Config.API_KEY or not Config.API_SECRET:
        pytest.skip("API credentials not available")


@pytest.fixture
def sample_orderbook_snapshot() -> dict:
    """Sample orderbook snapshot message."""
    return {
        "type": "l2_orderbook",
        "symbol": "BTCUSD",
        "timestamp": 1770465544198981,
        "last_sequence_no": 1088568,
        "product_id": 84,
        "spread": "14319.2",
        "buy": [
            {"depth": "1", "limit_price": "67924.0", "size": 1},
            {"depth": "1489", "limit_price": "66000.0", "size": 1488},
            {"depth": "1684", "limit_price": "61717.5", "size": 195},
        ],
        "sell": [
            {"depth": "18", "limit_price": "82243.2", "size": 18},
            {"depth": "24", "limit_price": "82944.8", "size": 6},
            {"depth": "74", "limit_price": "83166.8", "size": 50},
        ],
    }


@pytest.fixture
def sample_orderbook_update() -> dict:
    """Sample orderbook update message."""
    return {
        "type": "l2_orderbook",
        "symbol": "BTCUSD",
        "timestamp": 1770465544199000,
        "last_sequence_no": 1088569,
        "product_id": 84,
        "buy": [
            {"depth": "1", "limit_price": "67925.0", "size": 2},
        ],
        "sell": [
            {"depth": "18", "limit_price": "82243.2", "size": 0},  # Remove level
        ],
    }


# Pytest configuration
def pytest_configure(config):
    """Configure pytest."""
    # Add custom markers
    config.addinivalue_line("markers", "integration: mark test as integration test")
    config.addinivalue_line("markers", "live: mark test as requiring live connection")
    config.addinivalue_line(
        "markers", "credentials: mark test as requiring API credentials"
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection to add markers automatically."""
    for item in items:
        # Add 'live' marker to tests in integration_live module
        if "integration_live" in item.nodeid:
            item.add_marker(pytest.mark.live)
            item.add_marker(pytest.mark.credentials)
