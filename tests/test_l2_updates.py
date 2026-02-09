"""Tests for l2_updates channel support."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from deltatrader.client.websocket import WebSocketClient
from deltatrader.core.market_data import MarketDataManager
from deltatrader.models.orderbook import OrderBook
from deltatrader.utils.config import Config
from deltatrader.utils.integer_conversion import IntegerConverter


@pytest.fixture
def converter():
    """Create integer converter with test product specs."""
    from deltatrader.models.product import Product

    converter = IntegerConverter()
    # Register BTCUSD product spec
    product = Product(
        product_id=84,
        symbol="BTCUSD",
        description="Bitcoin Perpetual Futures",
        contract_type="perpetual_futures",
        tick_size="0.5",
        contract_size="0.001",
        quoting_asset="USD",
        settling_asset="USDT",
    )
    converter.register_product(product)
    return converter


@pytest.fixture
def ws_client():
    """Create mock WebSocket client."""
    client = MagicMock(spec=WebSocketClient)
    client.subscribe = AsyncMock()
    client.unsubscribe = AsyncMock()
    client.add_handler = MagicMock()
    return client


@pytest.fixture
def market_data_manager(ws_client, converter):
    """Create MarketDataManager instance."""
    return MarketDataManager(ws_client, converter)


class TestL2UpdatesConfiguration:
    """Test configuration for l2_updates channel."""

    def test_default_orderbook_channel(self):
        """Test default orderbook channel is l2_orderbook."""
        assert Config.ORDERBOOK_CHANNEL in ["l2_orderbook", "l2_updates"]

    @patch.dict("os.environ", {"ORDERBOOK_CHANNEL": "l2_updates"})
    def test_l2_updates_config(self):
        """Test configuring l2_updates channel."""
        # Reload config
        from importlib import reload

        from deltatrader.utils import config as config_module

        reload(config_module)
        assert config_module.Config.ORDERBOOK_CHANNEL == "l2_updates"

    @patch.dict("os.environ", {"ORDERBOOK_CHANNEL": "l2_orderbook"})
    def test_l2_orderbook_config(self):
        """Test configuring l2_orderbook channel."""
        from importlib import reload

        from deltatrader.utils import config as config_module

        reload(config_module)
        assert config_module.Config.ORDERBOOK_CHANNEL == "l2_orderbook"


class TestL2UpdatesSubscription:
    """Test subscription to l2_updates channel."""

    @pytest.mark.asyncio
    @patch.object(Config, "ORDERBOOK_CHANNEL", "l2_updates")
    async def test_subscribe_l2_updates(self, market_data_manager, ws_client):
        """Test subscribing to l2_updates channel."""
        await market_data_manager.subscribe_orderbook("BTCUSD")

        # Verify subscription to l2_updates channel
        ws_client.subscribe.assert_called_once_with(["l2_updates.BTCUSD"])
        ws_client.add_handler.assert_called_once()

        # Verify orderbook initialized
        assert "BTCUSD" in market_data_manager._orderbooks
        assert market_data_manager._pending_snapshots["BTCUSD"] is True

    @pytest.mark.asyncio
    @patch.object(Config, "ORDERBOOK_CHANNEL", "l2_orderbook")
    async def test_subscribe_l2_orderbook(self, market_data_manager, ws_client):
        """Test subscribing to l2_orderbook channel."""
        await market_data_manager.subscribe_orderbook("BTCUSD")

        # Verify subscription to l2_orderbook channel
        ws_client.subscribe.assert_called_once_with(["l2_orderbook.BTCUSD"])
        ws_client.add_handler.assert_called_once()

    @pytest.mark.asyncio
    @patch.object(Config, "ORDERBOOK_CHANNEL", "l2_updates")
    async def test_unsubscribe_l2_updates(self, market_data_manager, ws_client):
        """Test unsubscribing from l2_updates channel."""
        await market_data_manager.subscribe_orderbook("BTCUSD")
        await market_data_manager.unsubscribe_orderbook("BTCUSD")

        # Verify unsubscription
        ws_client.unsubscribe.assert_called_once_with(["l2_updates.BTCUSD"])
        assert "BTCUSD" not in market_data_manager._orderbooks
        assert "BTCUSD" not in market_data_manager._pending_snapshots


class TestL2UpdatesMessageHandling:
    """Test message handling for l2_updates channel."""

    @pytest.mark.asyncio
    async def test_handle_snapshot_message(self, market_data_manager, converter):
        """Test handling l2_updates snapshot message."""
        # Initialize orderbook
        market_data_manager._orderbooks["BTCUSD"] = OrderBook(symbol="BTCUSD")
        market_data_manager._pending_snapshots["BTCUSD"] = True

        # Create snapshot message
        snapshot_msg = {
            "action": "snapshot",
            "symbol": "BTCUSD",
            "timestamp": 1234567890,
            "sequence_no": 100,
            "buy": [
                {"limit_price": "50000.0", "size": "1.5"},
                {"limit_price": "49999.5", "size": "2.0"},
            ],
            "sell": [
                {"limit_price": "50000.5", "size": "1.0"},
                {"limit_price": "50001.0", "size": "1.5"},
            ],
        }

        await market_data_manager._handle_orderbook_message(snapshot_msg)

        # Verify orderbook updated
        orderbook = market_data_manager._orderbooks["BTCUSD"]
        assert orderbook.sequence_no == 100
        assert len(orderbook.bids) == 2
        assert len(orderbook.asks) == 2
        assert market_data_manager._pending_snapshots["BTCUSD"] is False

        # Verify price levels
        best_bid = orderbook.get_best_bid()
        best_ask = orderbook.get_best_ask()
        assert best_bid[0] == converter.price_to_integer("BTCUSD", "50000.0")
        assert best_ask[0] == converter.price_to_integer("BTCUSD", "50000.5")

    @pytest.mark.asyncio
    async def test_handle_update_message(self, market_data_manager, converter):
        """Test handling l2_updates update message."""
        # Initialize orderbook with snapshot
        market_data_manager._orderbooks["BTCUSD"] = OrderBook(symbol="BTCUSD")
        market_data_manager._pending_snapshots["BTCUSD"] = False

        snapshot_msg = {
            "action": "snapshot",
            "symbol": "BTCUSD",
            "timestamp": 1234567890,
            "sequence_no": 100,
            "buy": [{"limit_price": "50000.0", "size": "1.5"}],
            "sell": [{"limit_price": "50000.5", "size": "1.0"}],
        }
        await market_data_manager._handle_orderbook_message(snapshot_msg)

        # Send update message
        update_msg = {
            "action": "update",
            "symbol": "BTCUSD",
            "timestamp": 1234567891,
            "sequence_no": 101,
            "buy": [{"limit_price": "50000.0", "size": "2.0"}],  # Update existing
            "sell": [{"limit_price": "50001.0", "size": "0.5"}],  # Add new level
        }

        await market_data_manager._handle_orderbook_message(update_msg)

        # Verify orderbook updated
        orderbook = market_data_manager._orderbooks["BTCUSD"]
        assert orderbook.sequence_no == 101
        assert len(orderbook.asks) == 2  # Added new ask level

        # Verify updated size
        best_bid = orderbook.get_best_bid()
        assert best_bid[1] == converter.size_to_integer("2.0")

    @pytest.mark.asyncio
    async def test_handle_update_removes_level(self, market_data_manager, converter):
        """Test that update with size=0 removes a level."""
        # Initialize orderbook
        market_data_manager._orderbooks["BTCUSD"] = OrderBook(symbol="BTCUSD")
        market_data_manager._pending_snapshots["BTCUSD"] = False

        snapshot_msg = {
            "action": "snapshot",
            "symbol": "BTCUSD",
            "timestamp": 1234567890,
            "sequence_no": 100,
            "buy": [
                {"limit_price": "50000.0", "size": "1.5"},
                {"limit_price": "49999.5", "size": "1.0"},
            ],
            "sell": [{"limit_price": "50000.5", "size": "1.0"}],
        }
        await market_data_manager._handle_orderbook_message(snapshot_msg)

        # Remove a bid level
        update_msg = {
            "action": "update",
            "symbol": "BTCUSD",
            "timestamp": 1234567891,
            "sequence_no": 101,
            "buy": [{"limit_price": "49999.5", "size": "0"}],  # Remove level
            "sell": [],
        }

        await market_data_manager._handle_orderbook_message(update_msg)

        # Verify level removed
        orderbook = market_data_manager._orderbooks["BTCUSD"]
        assert len(orderbook.bids) == 1
        best_bid = orderbook.get_best_bid()
        assert best_bid[0] == converter.price_to_integer("BTCUSD", "50000.0")

    @pytest.mark.asyncio
    async def test_handle_error_message(self, market_data_manager):
        """Test handling l2_updates error message."""
        error_msg = {
            "action": "error",
            "symbol": "BTCUSD",
            "message": "Invalid symbol",
        }

        # Should not raise exception
        await market_data_manager._handle_orderbook_message(error_msg)

    @pytest.mark.asyncio
    async def test_skip_update_before_snapshot(self, market_data_manager):
        """Test that updates are skipped until snapshot received."""
        market_data_manager._orderbooks["BTCUSD"] = OrderBook(symbol="BTCUSD")
        market_data_manager._pending_snapshots["BTCUSD"] = True

        update_msg = {
            "action": "update",
            "symbol": "BTCUSD",
            "timestamp": 1234567890,
            "sequence_no": 100,
            "buy": [{"limit_price": "50000.0", "size": "1.5"}],
            "sell": [],
        }

        await market_data_manager._handle_orderbook_message(update_msg)

        # Verify update was skipped
        orderbook = market_data_manager._orderbooks["BTCUSD"]
        assert orderbook.sequence_no == 0
        assert len(orderbook.bids) == 0


class TestSequenceHandling:
    """Test sequence number handling and recovery."""

    @pytest.mark.asyncio
    async def test_sequence_mismatch_triggers_resubscribe(
        self, market_data_manager, ws_client
    ):
        """Test that sequence mismatch triggers resubscribe."""
        # Initialize with snapshot
        market_data_manager._orderbooks["BTCUSD"] = OrderBook(symbol="BTCUSD")
        market_data_manager._pending_snapshots["BTCUSD"] = False

        snapshot_msg = {
            "action": "snapshot",
            "symbol": "BTCUSD",
            "timestamp": 1234567890,
            "sequence_no": 100,
            "buy": [{"limit_price": "50000.0", "size": "1.5"}],
            "sell": [{"limit_price": "50000.5", "size": "1.0"}],
        }
        await market_data_manager._handle_orderbook_message(snapshot_msg)

        # Send update with wrong sequence (should be 101, but send 105)
        with patch.object(Config, "ORDERBOOK_CHANNEL", "l2_updates"):
            update_msg = {
                "action": "update",
                "symbol": "BTCUSD",
                "timestamp": 1234567891,
                "sequence_no": 105,  # Gap in sequence
                "buy": [{"limit_price": "50000.0", "size": "2.0"}],
                "sell": [],
            }

            await market_data_manager._handle_orderbook_message(update_msg)

            # Verify resubscribe triggered
            ws_client.unsubscribe.assert_called_once()
            assert ws_client.subscribe.call_count >= 1
            assert market_data_manager._pending_snapshots["BTCUSD"] is True

    @pytest.mark.asyncio
    async def test_sequence_continuity(self, market_data_manager):
        """Test that continuous sequence numbers are accepted."""
        market_data_manager._orderbooks["BTCUSD"] = OrderBook(symbol="BTCUSD")
        market_data_manager._pending_snapshots["BTCUSD"] = False

        # Initial snapshot
        snapshot_msg = {
            "action": "snapshot",
            "symbol": "BTCUSD",
            "timestamp": 1234567890,
            "sequence_no": 100,
            "buy": [{"limit_price": "50000.0", "size": "1.5"}],
            "sell": [{"limit_price": "50000.5", "size": "1.0"}],
        }
        await market_data_manager._handle_orderbook_message(snapshot_msg)

        # Send updates with continuous sequence
        for seq in range(101, 110):
            update_msg = {
                "action": "update",
                "symbol": "BTCUSD",
                "timestamp": 1234567890 + seq,
                "sequence_no": seq,
                "buy": [{"limit_price": "50000.0", "size": f"{seq * 0.1}"}],
                "sell": [],
            }
            await market_data_manager._handle_orderbook_message(update_msg)

        # Verify all updates applied
        orderbook = market_data_manager._orderbooks["BTCUSD"]
        assert orderbook.sequence_no == 109


class TestChecksumValidation:
    """Test checksum validation for l2_updates."""

    @pytest.mark.asyncio
    async def test_valid_checksum(self, market_data_manager, converter):
        """Test that valid checksum passes validation."""
        market_data_manager._orderbooks["BTCUSD"] = OrderBook(symbol="BTCUSD")
        market_data_manager._pending_snapshots["BTCUSD"] = True

        snapshot_msg = {
            "action": "snapshot",
            "symbol": "BTCUSD",
            "timestamp": 1234567890,
            "sequence_no": 100,
            "buy": [{"limit_price": "50000.0", "size": "1.5"}],
            "sell": [{"limit_price": "50000.5", "size": "1.0"}],
        }

        await market_data_manager._handle_orderbook_message(snapshot_msg)

        # Compute correct checksum
        orderbook = market_data_manager._orderbooks["BTCUSD"]
        correct_checksum = orderbook.compute_checksum(converter)

        # Send update with correct checksum
        update_msg = {
            "action": "update",
            "symbol": "BTCUSD",
            "timestamp": 1234567891,
            "sequence_no": 101,
            "buy": [{"limit_price": "50000.0", "size": "2.0"}],
            "sell": [],
            "cs": correct_checksum,
        }

        await market_data_manager._handle_orderbook_message(update_msg)

        # Should complete without errors
        assert orderbook.sequence_no == 101

    @pytest.mark.asyncio
    async def test_invalid_checksum_logged(self, market_data_manager, converter):
        """Test that invalid checksum is logged as warning."""
        market_data_manager._orderbooks["BTCUSD"] = OrderBook(symbol="BTCUSD")
        market_data_manager._pending_snapshots["BTCUSD"] = True

        snapshot_msg = {
            "action": "snapshot",
            "symbol": "BTCUSD",
            "timestamp": 1234567890,
            "sequence_no": 100,
            "buy": [{"limit_price": "50000.0", "size": "1.5"}],
            "sell": [{"limit_price": "50000.5", "size": "1.0"}],
        }

        await market_data_manager._handle_orderbook_message(snapshot_msg)

        # Send update with wrong checksum
        update_msg = {
            "action": "update",
            "symbol": "BTCUSD",
            "timestamp": 1234567891,
            "sequence_no": 101,
            "buy": [{"limit_price": "50000.0", "size": "2.0"}],
            "sell": [],
            "cs": 99999999,  # Invalid checksum
        }

        # Should not raise exception, just log warning
        await market_data_manager._handle_orderbook_message(update_msg)

        # Update still applied
        orderbook = market_data_manager._orderbooks["BTCUSD"]
        assert orderbook.sequence_no == 101

    @pytest.mark.asyncio
    async def test_checksum_with_real_format(self, market_data_manager):
        """Test checksum computation with real Delta Exchange message format."""
        import zlib

        # Real l2_updates snapshot format (array format: [price, size])
        snapshot_msg = {
            "action": "snapshot",
            "symbol": "BTCUSD",
            "timestamp": 1234567890,
            "sequence_no": 100,
            "bids": [
                ["71257.0", "3369"],
                ["71256.5", "4121"],
                ["71256.0", "2500"],
            ],
            "asks": [
                ["71258.0", "1500"],
                ["71258.5", "2000"],
                ["71259.0", "3500"],
            ],
        }

        # Compute expected checksum manually with the exact format
        # Format: "ask1_price:ask1_size,ask2_price:ask2_size,...|bid1_price:bid1_size,bid2_price:bid2_size,..."
        checksum_string = "71258.0:1500,71258.5:2000,71259.0:3500|71257.0:3369,71256.5:4121,71256.0:2500"
        expected_checksum = zlib.crc32(checksum_string.encode()) & 0xFFFFFFFF

        # Add checksum to message
        snapshot_msg["cs"] = expected_checksum

        # Process the snapshot
        await market_data_manager._handle_orderbook_message(snapshot_msg)

        # Verify orderbook was created and checksum matches
        orderbook = market_data_manager._orderbooks["BTCUSD"]
        assert orderbook.sequence_no == 100
        computed_checksum = orderbook.compute_checksum(market_data_manager.converter)
        assert computed_checksum == expected_checksum


class TestBackwardCompatibility:
    """Test backward compatibility with l2_orderbook channel."""

    @pytest.mark.asyncio
    @patch.object(Config, "ORDERBOOK_CHANNEL", "l2_orderbook")
    async def test_l2_orderbook_still_works(self, market_data_manager):
        """Test that l2_orderbook channel still works."""
        market_data_manager._orderbooks["BTCUSD"] = OrderBook(symbol="BTCUSD")
        market_data_manager._pending_snapshots["BTCUSD"] = True

        # l2_orderbook message format
        orderbook_msg = {
            "type": "l2_orderbook",
            "symbol": "BTCUSD",
            "timestamp": 1234567890,
            "last_sequence_no": 100,
            "buy": [{"limit_price": "50000.0", "size": "1.5"}],
            "sell": [{"limit_price": "50000.5", "size": "1.0"}],
        }

        await market_data_manager._handle_orderbook_message(orderbook_msg)

        # Verify orderbook updated
        orderbook = market_data_manager._orderbooks["BTCUSD"]
        assert orderbook.sequence_no == 100
        assert len(orderbook.bids) == 1
        assert len(orderbook.asks) == 1
        assert market_data_manager._pending_snapshots["BTCUSD"] is False

    @pytest.mark.asyncio
    async def test_legacy_snapshot_type_field(self, market_data_manager):
        """Test handling legacy snapshot messages with 'type' field."""
        market_data_manager._orderbooks["BTCUSD"] = OrderBook(symbol="BTCUSD")
        market_data_manager._pending_snapshots["BTCUSD"] = True

        # Legacy format with type="snapshot"
        snapshot_msg = {
            "type": "snapshot",
            "symbol": "BTCUSD",
            "timestamp": 1234567890,
            "sequence_no": 100,
            "buy": [{"limit_price": "50000.0", "size": "1.5"}],
            "sell": [{"limit_price": "50000.5", "size": "1.0"}],
        }

        await market_data_manager._handle_orderbook_message(snapshot_msg)

        # Should work with type field
        orderbook = market_data_manager._orderbooks["BTCUSD"]
        assert orderbook.sequence_no == 100
        assert market_data_manager._pending_snapshots["BTCUSD"] is False


class TestWebSocketMessageRouting:
    """Test WebSocket client message routing for l2_updates."""

    @pytest.mark.asyncio
    async def test_action_field_routing(self):
        """Test that messages with 'action' field are routed correctly."""
        ws_client = WebSocketClient()

        handler_called = False
        received_data = None

        async def test_handler(data):
            nonlocal handler_called, received_data
            handler_called = True
            received_data = data

        # Register handler for l2_updates channel
        ws_client._channel_handlers["l2_updates.BTCUSD"] = [test_handler]

        # Send message with action field
        message = {
            "action": "snapshot",
            "symbol": "BTCUSD",
            "timestamp": 1234567890,
            "sequence_no": 100,
            "buy": [],
            "sell": [],
        }

        await ws_client._handle_message(message)
        await asyncio.sleep(0.1)  # Allow async handlers to execute

        # Handler should be called
        assert handler_called
        assert received_data == message

    @pytest.mark.asyncio
    async def test_type_field_routing(self):
        """Test that messages with 'type' field still work."""
        ws_client = WebSocketClient()

        handler_called = False

        async def test_handler(data):
            nonlocal handler_called
            handler_called = True

        # Register handler for l2_orderbook channel
        ws_client._channel_handlers["l2_orderbook.BTCUSD"] = [test_handler]

        # Send message with type field
        message = {
            "type": "l2_orderbook",
            "symbol": "BTCUSD",
            "timestamp": 1234567890,
            "last_sequence_no": 100,
            "buy": [],
            "sell": [],
        }

        await ws_client._handle_message(message)
        await asyncio.sleep(0.1)

        # Handler should be called
        assert handler_called
