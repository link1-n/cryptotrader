"""Tests for trade message handling."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from deltatrader.core.market_data import MarketDataManager
from deltatrader.models.product import Product
from deltatrader.models.trade import Trade
from deltatrader.utils.integer_conversion import IntegerConverter


@pytest.fixture
def converter():
    """Create a converter with XRPUSD product registered."""
    conv = IntegerConverter()
    xrp_product = Product(
        symbol="XRPUSD",
        product_id=14969,
        description="XRP/USD Perpetual",
        contract_type="perpetual_futures",
        tick_size="0.0001",
        contract_size="1",
        quoting_asset="USD",
        settling_asset="USDT",
    )
    conv.register_product(xrp_product)
    return conv


@pytest.fixture
def market_data_manager(converter):
    """Create a market data manager instance."""
    ws_client = MagicMock()
    ws_client.subscribe = AsyncMock()
    ws_client.unsubscribe = AsyncMock()
    return MarketDataManager(ws_client, converter)


class TestTradeMessageHandling:
    """Test trade message handling for different formats."""

    @pytest.mark.asyncio
    async def test_handle_single_trade_message(self, market_data_manager):
        """Test handling a single trade message (most common format)."""
        # This is the actual format Delta Exchange sends
        trade_msg = {
            "buyer_role": "taker",
            "price": "1.4399",
            "product_id": 14969,
            "seller_role": "maker",
            "size": 2,
            "symbol": "XRPUSD",
            "timestamp": 1770576897065389,
            "type": "all_trades",
        }

        await market_data_manager._handle_trade_message(trade_msg)

        # Verify trade was stored
        trades = market_data_manager._trades.get("XRPUSD", [])
        assert len(trades) == 1

        # Verify trade details
        trade = trades[0]
        assert trade.symbol == "XRPUSD"
        assert trade.price == market_data_manager.converter.price_to_integer(
            "XRPUSD", "1.4399"
        )
        assert trade.size == market_data_manager.converter.size_to_integer("2")
        assert trade.timestamp == 1770576897065389
        assert trade.side == "buy"  # buyer_role = taker means buy

    @pytest.mark.asyncio
    async def test_handle_trade_array_format(self, market_data_manager):
        """Test handling trade messages with array format."""
        # Snapshot format with multiple trades
        trade_msg = {
            "type": "all_trades_snapshot",
            "symbol": "XRPUSD",
            "trades": [
                {
                    "buyer_role": "taker",
                    "price": "1.4399",
                    "size": 2,
                    "timestamp": 1770576897065389,
                },
                {
                    "buyer_role": "maker",
                    "price": "1.4398",
                    "size": 5,
                    "timestamp": 1770576897065390,
                },
            ],
        }

        await market_data_manager._handle_trade_message(trade_msg)

        # Verify both trades were stored
        trades = market_data_manager._trades.get("XRPUSD", [])
        assert len(trades) == 2

        # Verify first trade
        assert trades[0].side == "buy"
        assert trades[0].price == market_data_manager.converter.price_to_integer(
            "XRPUSD", "1.4399"
        )

        # Verify second trade
        assert trades[1].side == "sell"  # buyer_role = maker means sell
        assert trades[1].price == market_data_manager.converter.price_to_integer(
            "XRPUSD", "1.4398"
        )

    @pytest.mark.asyncio
    async def test_handle_multiple_sequential_trades(self, market_data_manager):
        """Test handling multiple sequential single trade messages."""
        # First trade
        trade_msg_1 = {
            "buyer_role": "taker",
            "price": "1.4399",
            "size": 2,
            "symbol": "XRPUSD",
            "timestamp": 1770576897065389,
            "type": "all_trades",
        }

        # Second trade
        trade_msg_2 = {
            "buyer_role": "maker",
            "price": "1.4398",
            "size": 3,
            "symbol": "XRPUSD",
            "timestamp": 1770576897065390,
            "type": "all_trades",
        }

        # Third trade
        trade_msg_3 = {
            "buyer_role": "taker",
            "price": "1.4400",
            "size": 1,
            "symbol": "XRPUSD",
            "timestamp": 1770576897065391,
            "type": "all_trades",
        }

        await market_data_manager._handle_trade_message(trade_msg_1)
        await market_data_manager._handle_trade_message(trade_msg_2)
        await market_data_manager._handle_trade_message(trade_msg_3)

        # Verify all trades were stored
        trades = market_data_manager._trades.get("XRPUSD", [])
        assert len(trades) == 3

        # Verify order (should be chronological)
        assert trades[0].timestamp == 1770576897065389
        assert trades[1].timestamp == 1770576897065390
        assert trades[2].timestamp == 1770576897065391

    @pytest.mark.asyncio
    async def test_trade_callback_triggered(self, market_data_manager):
        """Test that callbacks are triggered on trade updates."""
        callback_called = False
        received_trades = []

        async def on_trade_update(symbol: str, trades: list):
            nonlocal callback_called, received_trades
            callback_called = True
            received_trades = trades

        market_data_manager.add_trade_callback(on_trade_update)

        trade_msg = {
            "buyer_role": "taker",
            "price": "1.4399",
            "size": 2,
            "symbol": "XRPUSD",
            "timestamp": 1770576897065389,
            "type": "all_trades",
        }

        await market_data_manager._handle_trade_message(trade_msg)

        # Allow async callback to execute
        await asyncio.sleep(0.01)

        # Verify callback was called
        assert callback_called
        assert len(received_trades) == 1
        assert received_trades[0].symbol == "XRPUSD"

    @pytest.mark.asyncio
    async def test_trades_max_limit(self, market_data_manager):
        """Test that trades list is limited to max size."""
        # Default max is 1000, set lower for testing
        market_data_manager._max_trades_per_symbol = 5

        # Send 10 trades
        for i in range(10):
            trade_msg = {
                "buyer_role": "taker",
                "price": "1.4399",
                "size": 1,
                "symbol": "XRPUSD",
                "timestamp": 1770576897065389 + i,
                "type": "all_trades",
            }
            await market_data_manager._handle_trade_message(trade_msg)

        # Verify only last 5 are kept
        trades = market_data_manager._trades.get("XRPUSD", [])
        assert len(trades) == 5

        # Verify they are the most recent ones
        assert trades[0].timestamp == 1770576897065394
        assert trades[-1].timestamp == 1770576897065398

    @pytest.mark.asyncio
    async def test_trade_side_mapping(self, market_data_manager):
        """Test correct mapping of buyer_role to trade side."""
        # buyer_role = taker -> side = buy
        buy_trade_msg = {
            "buyer_role": "taker",
            "seller_role": "maker",
            "price": "1.4399",
            "size": 2,
            "symbol": "XRPUSD",
            "timestamp": 1770576897065389,
            "type": "all_trades",
        }

        await market_data_manager._handle_trade_message(buy_trade_msg)
        trades = market_data_manager._trades.get("XRPUSD", [])
        assert trades[0].side == "buy"

        # buyer_role = maker -> side = sell
        sell_trade_msg = {
            "buyer_role": "maker",
            "seller_role": "taker",
            "price": "1.4398",
            "size": 3,
            "symbol": "XRPUSD",
            "timestamp": 1770576897065390,
            "type": "all_trades",
        }

        await market_data_manager._handle_trade_message(sell_trade_msg)
        trades = market_data_manager._trades.get("XRPUSD", [])
        assert trades[1].side == "sell"

    @pytest.mark.asyncio
    async def test_missing_symbol_ignored(self, market_data_manager):
        """Test that messages without symbol are ignored."""
        trade_msg = {
            "buyer_role": "taker",
            "price": "1.4399",
            "size": 2,
            "timestamp": 1770576897065389,
            "type": "all_trades",
            # Missing symbol field
        }

        await market_data_manager._handle_trade_message(trade_msg)

        # No trades should be stored
        assert len(market_data_manager._trades) == 0

    @pytest.mark.asyncio
    async def test_get_trades_method(self, market_data_manager):
        """Test the get_trades accessor method."""
        # Add some trades
        trade_msg = {
            "buyer_role": "taker",
            "price": "1.4399",
            "size": 2,
            "symbol": "XRPUSD",
            "timestamp": 1770576897065389,
            "type": "all_trades",
        }

        await market_data_manager._handle_trade_message(trade_msg)

        # Get trades using accessor
        trades = market_data_manager.get_trades("XRPUSD")
        assert len(trades) == 1
        assert trades[0].symbol == "XRPUSD"

        # Get trades for non-existent symbol
        empty_trades = market_data_manager.get_trades("BTCUSD")
        assert len(empty_trades) == 0

    @pytest.mark.asyncio
    async def test_trade_with_string_size(self, market_data_manager):
        """Test handling trade with size as string."""
        trade_msg = {
            "buyer_role": "taker",
            "price": "1.4399",
            "size": "2",  # String instead of int
            "symbol": "XRPUSD",
            "timestamp": 1770576897065389,
            "type": "all_trades",
        }

        await market_data_manager._handle_trade_message(trade_msg)

        trades = market_data_manager._trades.get("XRPUSD", [])
        assert len(trades) == 1
        assert trades[0].size == market_data_manager.converter.size_to_integer("2")

    @pytest.mark.asyncio
    async def test_multiple_symbols(self, market_data_manager):
        """Test handling trades for multiple symbols simultaneously."""
        # Register BTCUSD
        btc_product = Product(
            symbol="BTCUSD",
            product_id=1,
            description="BTC/USD Perpetual",
            contract_type="perpetual_futures",
            tick_size="0.5",
            contract_size="1",
            quoting_asset="USD",
            settling_asset="USDT",
        )
        market_data_manager.converter.register_product(btc_product)

        # XRPUSD trade
        xrp_trade = {
            "buyer_role": "taker",
            "price": "1.4399",
            "size": 2,
            "symbol": "XRPUSD",
            "timestamp": 1770576897065389,
            "type": "all_trades",
        }

        # BTCUSD trade
        btc_trade = {
            "buyer_role": "maker",
            "price": "50000.0",
            "size": 1,
            "symbol": "BTCUSD",
            "timestamp": 1770576897065390,
            "type": "all_trades",
        }

        await market_data_manager._handle_trade_message(xrp_trade)
        await market_data_manager._handle_trade_message(btc_trade)

        # Verify both symbols have trades
        xrp_trades = market_data_manager.get_trades("XRPUSD")
        btc_trades = market_data_manager.get_trades("BTCUSD")

        assert len(xrp_trades) == 1
        assert len(btc_trades) == 1
        assert xrp_trades[0].symbol == "XRPUSD"
        assert btc_trades[0].symbol == "BTCUSD"
