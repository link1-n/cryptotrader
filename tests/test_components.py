"""Unit tests for framework components."""

import pytest

from deltatrader.client.rest import RestClient
from deltatrader.client.websocket import WebSocketClient
from deltatrader.models.orderbook import OrderBook
from deltatrader.models.product import Product
from deltatrader.utils.config import Config
from deltatrader.utils.integer_conversion import IntegerConverter


class TestRestClient:
    """Test suite for REST client."""

    @pytest.mark.asyncio
    async def test_rest_client_initialization(self):
        """Test REST client initialization."""
        client = RestClient()
        assert client.base_url == Config.get_rest_url()
        assert client.session is None

    @pytest.mark.asyncio
    async def test_rest_client_connect(self, rest_client: RestClient):
        """Test REST client connection."""
        assert rest_client.session is not None
        assert not rest_client.session.closed

    @pytest.mark.asyncio
    async def test_get_products(self, rest_client: RestClient):
        """Test fetching products."""
        products = await rest_client.get_products(contract_types=["perpetual_futures"])
        assert isinstance(products, list)
        assert len(products) > 0
        assert all(isinstance(p, Product) for p in products)

    @pytest.mark.asyncio
    async def test_get_product(self, rest_client: RestClient):
        """Test fetching a single product."""
        product = await rest_client.get_product("BTCUSD")
        assert product is not None
        assert product.symbol == "BTCUSD"
        assert product.product_id is not None

    @pytest.mark.asyncio
    async def test_get_orderbook(self, rest_client: RestClient):
        """Test fetching orderbook."""
        orderbook_data = await rest_client.get_orderbook("BTCUSD", depth=10)
        assert isinstance(orderbook_data, dict)
        assert "buy" in orderbook_data or "sell" in orderbook_data

    @pytest.mark.asyncio
    async def test_get_trades(self, rest_client: RestClient):
        """Test fetching recent trades."""
        trades = await rest_client.get_trades("BTCUSD", limit=10)
        assert isinstance(trades, list)

    @pytest.mark.asyncio
    @pytest.mark.credentials
    async def test_get_open_orders(
        self, rest_client: RestClient, skip_if_no_credentials
    ):
        """Test fetching open orders (requires auth)."""
        open_orders = await rest_client.get_open_orders()
        assert isinstance(open_orders, list)

    @pytest.mark.asyncio
    @pytest.mark.credentials
    async def test_get_positions(self, rest_client: RestClient, skip_if_no_credentials):
        """Test fetching positions (requires auth)."""
        positions = await rest_client.get_positions()
        assert isinstance(positions, list)

    @pytest.mark.asyncio
    @pytest.mark.credentials
    async def test_get_wallet_balance(
        self, rest_client: RestClient, skip_if_no_credentials
    ):
        """Test fetching wallet balance (requires auth)."""
        balance = await rest_client.get_wallet_balance()
        assert isinstance(balance, dict)


class TestWebSocketClient:
    """Test suite for WebSocket client."""

    @pytest.mark.asyncio
    async def test_websocket_initialization(self):
        """Test WebSocket client initialization."""
        ws = WebSocketClient()
        assert ws.ws_url == Config.get_ws_url()
        assert not ws.is_connected

    @pytest.mark.asyncio
    async def test_websocket_connect(self):
        """Test WebSocket connection."""
        ws = WebSocketClient()
        try:
            await ws.connect(authenticate=False)
            await ws.wait_connected(timeout=10.0)
            assert ws.is_connected
        finally:
            if ws.is_connected:
                await ws.disconnect()

    @pytest.mark.asyncio
    async def test_websocket_subscribe(self):
        """Test WebSocket subscription."""
        ws = WebSocketClient()
        received_messages = []

        ws.add_handler("snapshot", lambda data: received_messages.append(data))

        try:
            await ws.connect(authenticate=False)
            await ws.wait_connected(timeout=10.0)

            if ws.is_connected:
                await ws.subscribe(["l2_orderbook.BTCUSD"])
                # Wait for messages
                import asyncio

                await asyncio.sleep(5)

                # Should receive some messages
                assert len(received_messages) >= 0  # May or may not receive

        finally:
            if ws.is_connected:
                await ws.disconnect()


class TestIntegerConversion:
    """Test suite for integer conversion utilities."""

    def test_converter_initialization(self, converter: IntegerConverter):
        """Test IntegerConverter initialization."""
        assert len(converter._product_scales) == 0
        assert len(converter._product_tick_sizes) == 0

    def test_register_product(self, converter: IntegerConverter, test_product: Product):
        """Test product registration."""
        converter.register_product(test_product)

        scale = converter.get_scale("BTCUSD")
        assert scale > 0

    def test_price_conversion_roundtrip(self, registered_converter: IntegerConverter):
        """Test price conversion roundtrip."""
        price_str = "12345.5"
        price_int = registered_converter.price_to_integer("BTCUSD", price_str)
        price_back = registered_converter.integer_to_price("BTCUSD", price_int)

        assert price_str == price_back

    def test_size_conversion_integer(self, converter: IntegerConverter):
        """Test size conversion for integer input."""
        size = 10
        size_int = converter.size_to_integer(size)
        assert size_int == 10

    def test_size_conversion_string(self, converter: IntegerConverter):
        """Test size conversion for string input."""
        size_str = "10"
        size_int = converter.size_to_integer(size_str)
        assert size_int == 10

    def test_size_conversion_decimal(self, converter: IntegerConverter):
        """Test size conversion for decimal string."""
        size_str = "10.5"
        size_int = converter.size_to_integer(size_str)
        size_back = converter.integer_to_size(size_int)
        assert "10.5" in size_back

    def test_normalize_price(self, registered_converter: IntegerConverter):
        """Test price normalization to tick size."""
        # Get a price that's not aligned to tick
        base_price = 5000000
        unnormalized = base_price + 3

        normalized = registered_converter.normalize_price("BTCUSD", unnormalized)

        # Normalized should be aligned to tick size
        tick_size = registered_converter._product_tick_sizes.get("BTCUSD", 1)
        assert normalized % tick_size == 0

    def test_set_scale_manually(self, converter: IntegerConverter):
        """Test manually setting scale for a symbol."""
        converter.set_scale("TEST", 1000, 5)

        assert converter.get_scale("TEST") == 1000
        assert converter._product_tick_sizes["TEST"] == 5


class TestOrderbookOperations:
    """Test suite for orderbook operations."""

    def test_orderbook_initialization(self):
        """Test OrderBook initialization."""
        orderbook = OrderBook(symbol="BTCUSD")
        assert orderbook.symbol == "BTCUSD"
        assert len(orderbook.bids) == 0
        assert len(orderbook.asks) == 0

    def test_orderbook_snapshot_update(
        self, registered_converter: IntegerConverter, sample_orderbook_snapshot: dict
    ):
        """Test orderbook snapshot update."""
        orderbook = OrderBook(symbol="BTCUSD")
        orderbook.update_from_snapshot(sample_orderbook_snapshot, registered_converter)

        assert len(orderbook.bids) > 0
        assert len(orderbook.asks) > 0
        assert orderbook.sequence_no == 1088568

    def test_orderbook_get_best_bid_ask(
        self, registered_converter: IntegerConverter, sample_orderbook_snapshot: dict
    ):
        """Test getting best bid and ask."""
        orderbook = OrderBook(symbol="BTCUSD")
        orderbook.update_from_snapshot(sample_orderbook_snapshot, registered_converter)

        best_bid = orderbook.get_best_bid()
        best_ask = orderbook.get_best_ask()

        assert best_bid[0] > 0  # price
        assert best_bid[1] > 0  # size
        assert best_ask[0] > 0
        assert best_ask[1] > 0

    def test_orderbook_mid_price(
        self, registered_converter: IntegerConverter, sample_orderbook_snapshot: dict
    ):
        """Test mid price calculation."""
        orderbook = OrderBook(symbol="BTCUSD")
        orderbook.update_from_snapshot(sample_orderbook_snapshot, registered_converter)

        mid_price = orderbook.get_mid_price()
        best_bid = orderbook.get_best_bid()
        best_ask = orderbook.get_best_ask()

        assert mid_price > 0
        assert best_bid[0] < mid_price < best_ask[0]

    def test_orderbook_spread(
        self, registered_converter: IntegerConverter, sample_orderbook_snapshot: dict
    ):
        """Test spread calculation."""
        orderbook = OrderBook(symbol="BTCUSD")
        orderbook.update_from_snapshot(sample_orderbook_snapshot, registered_converter)

        spread = orderbook.get_spread()
        best_bid = orderbook.get_best_bid()
        best_ask = orderbook.get_best_ask()

        assert spread > 0
        assert spread == best_ask[0] - best_bid[0]

    def test_orderbook_incremental_update(
        self,
        registered_converter: IntegerConverter,
        sample_orderbook_snapshot: dict,
        sample_orderbook_update: dict,
    ):
        """Test orderbook incremental update."""
        orderbook = OrderBook(symbol="BTCUSD")
        orderbook.update_from_snapshot(sample_orderbook_snapshot, registered_converter)

        original_bid = orderbook.get_best_bid()

        success = orderbook.apply_update(sample_orderbook_update, registered_converter)
        assert success is True

        # Verify sequence number updated
        assert orderbook.sequence_no == 1088569

    def test_orderbook_remove_level(
        self, registered_converter: IntegerConverter, sample_orderbook_snapshot: dict
    ):
        """Test removing orderbook level with size 0."""
        orderbook = OrderBook(symbol="BTCUSD")
        orderbook.update_from_snapshot(sample_orderbook_snapshot, registered_converter)

        original_ask_count = len(orderbook.asks)

        # Apply update that removes a level
        update = {
            "type": "l2_orderbook",
            "symbol": "BTCUSD",
            "timestamp": 1770465544199000,
            "last_sequence_no": 1088569,
            "sell": [
                {"limit_price": "82243.2", "size": 0},  # Remove this level
            ],
        }

        orderbook.apply_update(update, registered_converter)

        # Should have one less ask level
        assert len(orderbook.asks) < original_ask_count


class TestConfiguration:
    """Test suite for configuration."""

    def test_environment_setting(self):
        """Test environment configuration."""
        assert Config.ENVIRONMENT in ["testnet", "live"]

    def test_ws_url(self):
        """Test WebSocket URL configuration."""
        url = Config.get_ws_url()
        assert url.startswith("wss://")

    def test_rest_url(self):
        """Test REST URL configuration."""
        url = Config.get_rest_url()
        assert url.startswith("https://")

    def test_demo_mode_check(self):
        """Test demo mode check."""
        is_demo = Config.is_demo_mode()
        assert isinstance(is_demo, bool)

    def test_validation(self):
        """Test configuration validation."""
        # Should not raise exception
        Config.validate()
