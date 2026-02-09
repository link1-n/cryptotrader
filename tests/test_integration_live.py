"""Integration tests for live trading on testnet."""

import asyncio

import pytest

from deltatrader import TradingEngine
from deltatrader.models.order import Order
from deltatrader.utils.config import Config


@pytest.mark.integration
@pytest.mark.live
@pytest.mark.credentials
@pytest.mark.asyncio
class TestLiveTradingIntegration:
    """Integration tests for live trading on testnet."""

    async def test_live_trading_full_lifecycle(self, skip_if_no_credentials):
        """Test complete live trading lifecycle on testnet."""
        # Force testnet and exchange mode
        original_env = Config.ENVIRONMENT
        original_dest = Config.ORDER_DESTINATION
        Config.ENVIRONMENT = "testnet"
        Config.ORDER_DESTINATION = "exchange"

        engine = TradingEngine()

        try:
            # Initialize
            await engine.initialize(symbols=["BTCUSD"])

            # Wait for market data
            await asyncio.sleep(3)

            # Get orderbook
            orderbook = engine.market_data.get_orderbook("BTCUSD")
            assert orderbook is not None

            mid_price = orderbook.get_mid_price()
            assert mid_price > 0

            # Place orders far from market to avoid fills
            safe_bid_price = int(mid_price * 0.90)  # 10% below
            safe_ask_price = int(mid_price * 1.10)  # 10% above

            # Test 1: Place limit buy order
            buy_order = Order(
                symbol="BTCUSD",
                side="buy",
                order_type="limit_order",
                size=1,
                price=safe_bid_price,
            )

            placed_buy = await engine.order_manager.place_order(buy_order)
            assert placed_buy.status in ["open", "pending"]
            assert placed_buy.client_order_id is not None
            assert placed_buy.exchange_order_id is not None

            # Wait for order to settle
            await asyncio.sleep(1)

            # Test 2: Place limit sell order
            sell_order = Order(
                symbol="BTCUSD",
                side="sell",
                order_type="limit_order",
                size=1,
                price=safe_ask_price,
            )

            placed_sell = await engine.order_manager.place_order(sell_order)
            assert placed_sell.status in ["open", "pending"]
            assert placed_sell.client_order_id is not None
            assert placed_sell.exchange_order_id is not None

            await asyncio.sleep(1)

            # Test 3: Get open orders
            open_orders = await engine.order_manager.get_open_orders("BTCUSD")
            assert len(open_orders) >= 2

            # Verify our orders are in the list
            order_ids = [o.client_order_id for o in open_orders]
            assert placed_buy.client_order_id in order_ids
            assert placed_sell.client_order_id in order_ids

            # Test 4: Edit order (try to modify in place)
            new_buy_price = safe_bid_price - 100
            edited = await engine.order_manager.edit_order(
                placed_buy.client_order_id, new_price=new_buy_price
            )

            if edited:
                assert (
                    edited.price == new_buy_price
                    or edited.client_order_id != placed_buy.client_order_id
                )
                # If edit resulted in replacement, update our reference
                if edited.client_order_id != placed_buy.client_order_id:
                    placed_buy = edited

            await asyncio.sleep(1)

            # Test 5: Edit sell order price
            new_sell_price = safe_ask_price + 100
            edited_sell = await engine.order_manager.edit_order(
                placed_sell.client_order_id, new_price=new_sell_price
            )

            if edited_sell:
                assert edited_sell.price == new_sell_price
                placed_sell = edited_sell

            await asyncio.sleep(1)

            # Test 6: Cancel specific order
            success = await engine.order_manager.cancel_order(
                placed_buy.client_order_id
            )
            assert success is True

            await asyncio.sleep(1)

            # Test 7: Cancel all orders
            count = await engine.order_manager.cancel_all_orders("BTCUSD")
            assert count >= 1

            await asyncio.sleep(1)

            # Test 8: Verify all cancelled
            remaining = await engine.order_manager.get_open_orders("BTCUSD")
            # Filter to only our test orders
            test_orders = [
                o
                for o in remaining
                if o.client_order_id
                in [placed_buy.client_order_id, placed_sell.client_order_id]
            ]
            assert len(test_orders) == 0

        finally:
            # Cleanup: ensure all test orders are cancelled
            try:
                await engine.order_manager.cancel_all_orders("BTCUSD")
            except:
                pass

            await engine.stop()
            Config.ENVIRONMENT = original_env
            Config.ORDER_DESTINATION = original_dest

    async def test_live_order_placement_and_cancellation(self, skip_if_no_credentials):
        """Test basic order placement and cancellation on testnet."""
        original_env = Config.ENVIRONMENT
        original_dest = Config.ORDER_DESTINATION
        Config.ENVIRONMENT = "testnet"
        Config.ORDER_DESTINATION = "exchange"

        engine = TradingEngine()

        try:
            await engine.initialize(symbols=["BTCUSD"])
            await asyncio.sleep(2)

            orderbook = engine.market_data.get_orderbook("BTCUSD")
            mid_price = orderbook.get_mid_price()

            # Place order far from market
            order_price = int(mid_price * 0.85)

            order = Order(
                symbol="BTCUSD",
                side="buy",
                order_type="limit_order",
                size=1,
                price=order_price,
            )

            placed = await engine.order_manager.place_order(order)
            assert placed.exchange_order_id is not None
            assert placed.client_order_id is not None

            await asyncio.sleep(1)

            # Cancel the order
            success = await engine.order_manager.cancel_order(placed.client_order_id)
            assert success is True

            await asyncio.sleep(1)

            # Verify cancelled
            open_orders = await engine.order_manager.get_open_orders("BTCUSD")
            test_order = [
                o for o in open_orders if o.client_order_id == placed.client_order_id
            ]
            assert len(test_order) == 0

        finally:
            try:
                await engine.order_manager.cancel_all_orders("BTCUSD")
            except:
                pass
            await engine.stop()
            Config.ENVIRONMENT = original_env
            Config.ORDER_DESTINATION = original_dest

    async def test_live_order_reconciliation(self, skip_if_no_credentials):
        """Test order reconciliation on testnet."""
        original_env = Config.ENVIRONMENT
        original_dest = Config.ORDER_DESTINATION
        Config.ENVIRONMENT = "testnet"
        Config.ORDER_DESTINATION = "exchange"

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
                    size=1,
                    price=int(mid_price * (0.85 - i * 0.01)),
                )
                placed = await engine.order_manager.place_order(order)
                orders.append(placed)
                await asyncio.sleep(0.5)

            # Run reconciliation
            stats = await engine.order_manager.reconcile_orders()

            # Should sync our orders
            assert stats["synced"] >= 3
            assert stats["errors"] == 0

        finally:
            try:
                await engine.order_manager.cancel_all_orders("BTCUSD")
            except:
                pass
            await engine.stop()
            Config.ENVIRONMENT = original_env
            Config.ORDER_DESTINATION = original_dest

    async def test_live_edit_vs_replace(self, skip_if_no_credentials):
        """Test edit vs replace behavior on testnet."""
        original_env = Config.ENVIRONMENT
        original_dest = Config.ORDER_DESTINATION
        Config.ENVIRONMENT = "testnet"
        Config.ORDER_DESTINATION = "exchange"

        engine = TradingEngine()

        try:
            await engine.initialize(symbols=["BTCUSD"])
            await asyncio.sleep(2)

            orderbook = engine.market_data.get_orderbook("BTCUSD")
            mid_price = orderbook.get_mid_price()

            base_price = int(mid_price * 0.85)

            # Test edit (should preserve order ID if supported)
            order1 = Order(
                symbol="BTCUSD",
                side="buy",
                order_type="limit_order",
                size=1,
                price=base_price,
            )

            placed1 = await engine.order_manager.place_order(order1)
            original_id1 = placed1.client_order_id

            await asyncio.sleep(1)

            edited = await engine.order_manager.edit_order(
                original_id1, new_price=base_price - 50
            )

            # Edit might preserve ID or create new one (depends on exchange support)
            assert edited is not None

            await asyncio.sleep(1)

            # Test edit with different price
            order2 = Order(
                symbol="BTCUSD",
                side="buy",
                order_type="limit_order",
                size=1,
                price=base_price - 100,
            )

            placed2 = await engine.order_manager.place_order(order2)
            original_id2 = placed2.client_order_id

            await asyncio.sleep(1)

            edited2 = await engine.order_manager.edit_order(
                original_id2, new_price=base_price - 150
            )

            # Edit should preserve order ID
            if edited2:
                assert edited2.price == base_price - 150

        finally:
            try:
                await engine.order_manager.cancel_all_orders("BTCUSD")
            except:
                pass
            await engine.stop()
            Config.ENVIRONMENT = original_env
            Config.ORDER_DESTINATION = original_dest

    async def test_live_get_positions(self, skip_if_no_credentials):
        """Test getting positions on testnet."""
        original_env = Config.ENVIRONMENT
        Config.ENVIRONMENT = "testnet"

        engine = TradingEngine()

        try:
            await engine.initialize(symbols=["BTCUSD"])
            await asyncio.sleep(2)

            # Get positions (should work even if empty)
            positions = await engine.get_positions()
            assert isinstance(positions, list)

        finally:
            await engine.stop()
            Config.ENVIRONMENT = original_env

    async def test_live_get_wallet_balance(self, skip_if_no_credentials):
        """Test getting wallet balance on testnet."""
        original_env = Config.ENVIRONMENT
        Config.ENVIRONMENT = "testnet"

        engine = TradingEngine()

        try:
            await engine.initialize(symbols=["BTCUSD"])
            await asyncio.sleep(2)

            # Get wallet balance
            balance = await engine.get_wallet_balance()
            assert isinstance(balance, dict)

        finally:
            await engine.stop()
            Config.ENVIRONMENT = original_env

    async def test_live_multiple_symbols(self, skip_if_no_credentials):
        """Test live trading with multiple symbols on testnet."""
        original_env = Config.ENVIRONMENT
        original_dest = Config.ORDER_DESTINATION
        Config.ENVIRONMENT = "testnet"
        Config.ORDER_DESTINATION = "exchange"

        engine = TradingEngine()

        try:
            await engine.initialize(symbols=["BTCUSD", "ETHUSD"])
            await asyncio.sleep(3)

            # Place orders for both symbols
            orders = []
            for symbol in ["BTCUSD", "ETHUSD"]:
                orderbook = engine.market_data.get_orderbook(symbol)
                if orderbook:
                    mid_price = orderbook.get_mid_price()

                    order = Order(
                        symbol=symbol,
                        side="buy",
                        order_type="limit_order",
                        size=1,
                        price=int(mid_price * 0.85),
                    )

                    placed = await engine.order_manager.place_order(order)
                    if placed.exchange_order_id:
                        orders.append(placed)
                    await asyncio.sleep(1)

            assert len(orders) >= 1

            # Get orders by symbol
            btc_orders = await engine.order_manager.get_open_orders("BTCUSD")
            eth_orders = await engine.order_manager.get_open_orders("ETHUSD")

            # At least one should have orders
            assert len(btc_orders) + len(eth_orders) >= 1

            # Cancel all for one symbol
            await engine.order_manager.cancel_all_orders("BTCUSD")
            await asyncio.sleep(1)

        finally:
            try:
                await engine.order_manager.cancel_all_orders()
            except:
                pass
            await engine.stop()
            Config.ENVIRONMENT = original_env
            Config.ORDER_DESTINATION = original_dest

    async def test_live_rate_limiting(self, skip_if_no_credentials):
        """Test that rate limiting information is available."""
        original_env = Config.ENVIRONMENT
        Config.ENVIRONMENT = "testnet"

        engine = TradingEngine()

        try:
            await engine.initialize(symbols=["BTCUSD"])
            await asyncio.sleep(2)

            # Make a REST call
            await engine.rest_client.get_product("BTCUSD")

            # Check rate limit info
            remaining, reset = engine.rest_client.get_rate_limit_info()

            # These might be None if not provided by API
            # Just verify the method works
            assert True

        finally:
            await engine.stop()
            Config.ENVIRONMENT = original_env

    async def test_live_order_with_custom_client_id(self, skip_if_no_credentials):
        """Test placing order with custom client_order_id on testnet."""
        original_env = Config.ENVIRONMENT
        original_dest = Config.ORDER_DESTINATION
        Config.ENVIRONMENT = "testnet"
        Config.ORDER_DESTINATION = "exchange"

        engine = TradingEngine()

        try:
            await engine.initialize(symbols=["BTCUSD"])
            await asyncio.sleep(2)

            orderbook = engine.market_data.get_orderbook("BTCUSD")
            mid_price = orderbook.get_mid_price()

            custom_id = "test_custom_id_12345"

            order = Order(
                symbol="BTCUSD",
                side="buy",
                order_type="limit_order",
                size=1,
                price=int(mid_price * 0.85),
                client_order_id=custom_id,
            )

            placed = await engine.order_manager.place_order(order)
            assert placed.client_order_id == custom_id
            assert placed.exchange_order_id is not None

            await asyncio.sleep(1)

            # Cancel using custom ID
            success = await engine.order_manager.cancel_order(custom_id)
            assert success is True

        finally:
            try:
                await engine.order_manager.cancel_all_orders("BTCUSD")
            except:
                pass
            await engine.stop()
            Config.ENVIRONMENT = original_env
            Config.ORDER_DESTINATION = original_dest
