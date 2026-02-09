"""Integration tests for paper trading mode."""

import asyncio

import pytest

from deltatrader import TradingEngine
from deltatrader.models.order import Order
from deltatrader.utils.config import Config


@pytest.mark.integration
@pytest.mark.asyncio
class TestPaperTradingIntegration:
    """Integration tests for paper trading."""

    async def test_paper_trading_full_lifecycle(self):
        """Test complete paper trading lifecycle."""
        # Force paper trading mode
        original_dest = Config.ORDER_DESTINATION
        Config.ORDER_DESTINATION = "paper"

        engine = TradingEngine()

        try:
            # Initialize
            await engine.initialize(symbols=["BTCUSD"])

            # Wait for market data
            await asyncio.sleep(2)

            # Get orderbook
            orderbook = engine.market_data.get_orderbook("BTCUSD")
            assert orderbook is not None

            mid_price = orderbook.get_mid_price()
            assert mid_price > 0

            # Test 1: Place limit buy order
            buy_price = mid_price - 1000
            buy_order = Order(
                symbol="BTCUSD",
                side="buy",
                order_type="limit_order",
                size=1,
                price=buy_price,
            )

            placed_buy = await engine.order_manager.place_order(buy_order)
            assert placed_buy.status == "open"
            assert placed_buy.client_order_id is not None

            # Test 2: Place limit sell order
            sell_price = mid_price + 1000
            sell_order = Order(
                symbol="BTCUSD",
                side="sell",
                order_type="limit_order",
                size=1,
                price=sell_price,
            )

            placed_sell = await engine.order_manager.place_order(sell_order)
            assert placed_sell.status == "open"
            assert placed_sell.client_order_id is not None

            # Test 3: Get open orders
            open_orders = await engine.order_manager.get_open_orders("BTCUSD")
            assert len(open_orders) >= 2

            # Test 4: Edit order
            new_buy_price = buy_price - 100
            edited = await engine.order_manager.edit_order(
                placed_buy.client_order_id, new_price=new_buy_price
            )
            assert edited is not None
            assert edited.price == new_buy_price

            # Test 5: Edit sell order price
            new_sell_price = sell_price + 100
            edited_sell = await engine.order_manager.edit_order(
                placed_sell.client_order_id, new_price=new_sell_price
            )
            assert edited_sell is not None
            assert edited_sell.price == new_sell_price

            # Test 6: Cancel specific order
            success = await engine.order_manager.cancel_order(
                placed_buy.client_order_id
            )
            assert success is True

            # Test 7: Cancel all orders
            count = await engine.order_manager.cancel_all_orders("BTCUSD")
            assert count >= 1

            # Test 8: Verify all cancelled
            remaining = await engine.order_manager.get_open_orders("BTCUSD")
            assert len(remaining) == 0

        finally:
            await engine.stop()
            Config.ORDER_DESTINATION = original_dest

    async def test_paper_market_order_auto_fill(self):
        """Test that market orders auto-fill in paper mode."""
        original_dest = Config.ORDER_DESTINATION
        Config.ORDER_DESTINATION = "paper"

        engine = TradingEngine()

        try:
            await engine.initialize(symbols=["BTCUSD"])
            await asyncio.sleep(2)

            # Place market order
            market_order = Order(
                symbol="BTCUSD",
                side="buy",
                order_type="market_order",
                size=1,
            )

            placed = await engine.order_manager.place_order(market_order)
            assert placed.client_order_id is not None

            # Wait for auto-fill
            await asyncio.sleep(0.2)

            # Check order is filled
            filled_order = engine.order_manager.get_order(placed.client_order_id)
            assert filled_order.status == "filled"
            assert filled_order.filled_size == filled_order.size

        finally:
            await engine.stop()
            Config.ORDER_DESTINATION = original_dest

    async def test_paper_manual_fill_simulation(self):
        """Test manual fill simulation in paper mode."""
        original_dest = Config.ORDER_DESTINATION
        Config.ORDER_DESTINATION = "paper"

        engine = TradingEngine()

        try:
            await engine.initialize(symbols=["BTCUSD"])
            await asyncio.sleep(2)

            orderbook = engine.market_data.get_orderbook("BTCUSD")
            mid_price = orderbook.get_mid_price()

            # Place limit order
            order = Order(
                symbol="BTCUSD",
                side="buy",
                order_type="limit_order",
                size=5,
                price=mid_price - 500,
            )

            placed = await engine.order_manager.place_order(order)

            # Manually simulate fill
            fill_price = mid_price - 500
            success = engine.order_manager.simulate_fill(
                placed.client_order_id, fill_price
            )
            assert success is True

            # Verify filled
            filled = engine.order_manager.get_order(placed.client_order_id)
            assert filled.status == "filled"
            assert filled.filled_size == 5
            assert filled.average_fill_price == fill_price

        finally:
            await engine.stop()
            Config.ORDER_DESTINATION = original_dest

    async def test_paper_order_reconciliation(self):
        """Test order reconciliation in paper mode."""
        original_dest = Config.ORDER_DESTINATION
        Config.ORDER_DESTINATION = "paper"

        engine = TradingEngine()

        try:
            await engine.initialize(symbols=["BTCUSD"])
            await asyncio.sleep(2)

            orderbook = engine.market_data.get_orderbook("BTCUSD")
            mid_price = orderbook.get_mid_price()

            # Place multiple orders
            orders = []
            for i in range(3):
                order = Order(
                    symbol="BTCUSD",
                    side="buy",
                    order_type="limit_order",
                    size=i + 1,
                    price=mid_price - (100 * (i + 1)),
                )
                placed = await engine.order_manager.place_order(order)
                orders.append(placed)

            # Run reconciliation
            stats = await engine.order_manager.reconcile_orders()

            # All orders should be synced
            assert stats["synced"] == 3
            assert stats["errors"] == 0

        finally:
            await engine.stop()
            Config.ORDER_DESTINATION = original_dest

    async def test_paper_multiple_symbols(self):
        """Test paper trading with multiple symbols."""
        original_dest = Config.ORDER_DESTINATION
        Config.ORDER_DESTINATION = "paper"

        engine = TradingEngine()

        try:
            await engine.initialize(symbols=["BTCUSD", "ETHUSD"])
            await asyncio.sleep(3)

            # Place orders for both symbols
            for symbol in ["BTCUSD", "ETHUSD"]:
                orderbook = engine.market_data.get_orderbook(symbol)
                if orderbook:
                    mid_price = orderbook.get_mid_price()

                    order = Order(
                        symbol=symbol,
                        side="buy",
                        order_type="limit_order",
                        size=1,
                        price=mid_price - 100,
                    )

                    placed = await engine.order_manager.place_order(order)
                    assert placed.status == "open"

            # Get orders by symbol
            btc_orders = await engine.order_manager.get_open_orders("BTCUSD")
            eth_orders = await engine.order_manager.get_open_orders("ETHUSD")

            assert len(btc_orders) >= 1
            assert len(eth_orders) >= 1

            # Cancel all for one symbol
            await engine.order_manager.cancel_all_orders("BTCUSD")

            # Verify
            btc_orders_after = await engine.order_manager.get_open_orders("BTCUSD")
            eth_orders_after = await engine.order_manager.get_open_orders("ETHUSD")

            assert len(btc_orders_after) == 0
            assert len(eth_orders_after) >= 1

        finally:
            await engine.stop()
            Config.ORDER_DESTINATION = original_dest

    async def test_paper_order_latency_simulation(self):
        """Test that paper trading simulates latency."""
        original_dest = Config.ORDER_DESTINATION
        Config.ORDER_DESTINATION = "paper"

        engine = TradingEngine()

        try:
            await engine.initialize(symbols=["BTCUSD"])
            await asyncio.sleep(2)

            orderbook = engine.market_data.get_orderbook("BTCUSD")
            mid_price = orderbook.get_mid_price()

            order = Order(
                symbol="BTCUSD",
                side="buy",
                order_type="limit_order",
                size=1,
                price=mid_price - 100,
            )

            # Measure time
            import time

            start = time.time()
            await engine.order_manager.place_order(order)
            elapsed = time.time() - start

            # Should take at least the simulated latency (50ms)
            assert elapsed >= 0.05

        finally:
            await engine.stop()
            Config.ORDER_DESTINATION = original_dest

    async def test_paper_order_edit_atomicity(self):
        """Test that edit_order is atomic in paper mode."""
        original_dest = Config.ORDER_DESTINATION
        Config.ORDER_DESTINATION = "paper"

        engine = TradingEngine()

        try:
            await engine.initialize(symbols=["BTCUSD"])
            await asyncio.sleep(2)

            orderbook = engine.market_data.get_orderbook("BTCUSD")
            mid_price = orderbook.get_mid_price()

            # Place order
            order = Order(
                symbol="BTCUSD",
                side="buy",
                order_type="limit_order",
                size=5,
                price=mid_price - 100,
            )

            old_order = await engine.order_manager.place_order(order)
            old_id = old_order.client_order_id

            # Edit order
            edited_order = await engine.order_manager.edit_order(
                old_id, new_size=10, new_price=mid_price - 200
            )

            # Verify order was edited (not replaced)
            assert edited_order is not None
            assert edited_order.status == "open"
            assert edited_order.size == 10
            assert edited_order.price == mid_price - 200
            assert edited_order.client_order_id == old_id  # Same ID, edited in place

        finally:
            await engine.stop()
            Config.ORDER_DESTINATION = original_dest

    async def test_paper_get_all_orders(self):
        """Test getting all orders including closed ones."""
        original_dest = Config.ORDER_DESTINATION
        Config.ORDER_DESTINATION = "paper"

        engine = TradingEngine()

        try:
            await engine.initialize(symbols=["BTCUSD"])
            await asyncio.sleep(2)

            orderbook = engine.market_data.get_orderbook("BTCUSD")
            mid_price = orderbook.get_mid_price()

            # Place multiple orders
            orders = []
            for i in range(3):
                order = Order(
                    symbol="BTCUSD",
                    side="buy",
                    order_type="limit_order",
                    size=i + 1,
                    price=mid_price - (100 * (i + 1)),
                )
                placed = await engine.order_manager.place_order(order)
                orders.append(placed)

            # Cancel one
            await engine.order_manager.cancel_order(orders[0].client_order_id)

            # Get all orders
            all_orders = engine.order_manager.get_all_orders()

            # Should include cancelled order
            assert len(all_orders) >= 3
            cancelled_count = sum(1 for o in all_orders if o.status == "cancelled")
            assert cancelled_count >= 1

        finally:
            await engine.stop()
            Config.ORDER_DESTINATION = original_dest
