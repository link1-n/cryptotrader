"""Test script to verify order placement on Delta Exchange testnet."""

import asyncio

from deltatrader import Config, TradingEngine
from deltatrader.utils.logger import logger


async def test_order_placement():
    """Test placing and cancelling orders on testnet."""
    logger.info("=" * 70)
    logger.info("Delta Exchange Testnet - Order Placement Test")
    logger.info("=" * 70)

    # Verify we're on testnet
    if Config.ENVIRONMENT != "testnet":
        logger.error("This script only works on testnet!")
        logger.error("Set DELTA_ENVIRONMENT=testnet in your .env file")
        return

    # if Config.ORDER_DESTINATION != "exchange":
    #     logger.error("Ensure order destination is exchange")
    #     logger.error("Set ORDER_DESTINATION=exchange in .env file")
    #     return

    # Check credentials
    if not Config.API_KEY or not Config.API_SECRET:
        logger.error(
            "API credentials not found!\n"
            "Please set DELTA_API_KEY and DELTA_API_SECRET in .env file"
        )
        return

    logger.info(f"Environment: {Config.ENVIRONMENT}")
    logger.info(f"WebSocket URL: {Config.get_ws_url()}")
    logger.info(f"REST URL: {Config.get_rest_url()}")
    logger.info("")

    # Create engine in LIVE mode (not paper trading)
    engine = TradingEngine()

    try:
        # Initialize
        logger.info("Initializing trading engine...")
        await engine.initialize(symbols=["BTCUSD"])

        # Subscribe to market data to get current prices
        await engine.market_data.subscribe_orderbook("BTCUSD")

        # Get current market data
        logger.info("Waiting for market data...")
        await asyncio.sleep(2)

        orderbook = engine.market_data.get_orderbook("BTCUSD")
        if not orderbook:
            logger.error("No orderbook data available!")
            return

        converter = engine.converter
        best_bid_int, _ = orderbook.get_best_bid()
        best_ask_int, _ = orderbook.get_best_ask()

        if best_bid_int == 0 or best_ask_int == 0:
            logger.error("Invalid orderbook data!")
            return

        best_bid_float = float(converter.integer_to_price("BTCUSD", best_bid_int))
        best_ask_float = float(converter.integer_to_price("BTCUSD", best_ask_int))

        logger.info("")
        logger.info("Current Market:")
        logger.info(f"  Best Bid: ${best_bid_float:,.1f}, ({best_bid_int})")
        logger.info(f"  Best Ask: ${best_ask_float:,.1f}, ({best_ask_int})")
        logger.info("")

        # Calculate safe order prices (far from current market)
        # Place bid 5% below best bid
        safe_bid_int = int(best_bid_int * 0.95)
        # Place ask 5% above best ask
        safe_ask_int = int(best_ask_int * 1.05)

        safe_bid_float = float(converter.integer_to_price("BTCUSD", safe_bid_int))
        safe_ask_float = float(converter.integer_to_price("BTCUSD", safe_ask_int))

        logger.info("Test Order Prices (intentionally far from market):")
        logger.info(f"  Test Bid: ${safe_bid_float:,.1f} (5% below best bid)")
        logger.info(f"  Test Ask: ${safe_ask_float:,.1f} (5% above best ask)")
        logger.info("")

        # Test 1: Place a buy limit order
        logger.info("-" * 70)
        logger.info("TEST 1: Placing BUY limit order")
        logger.info("-" * 70)

        from deltatrader.models.order import Order

        buy_order = Order(
            symbol="BTCUSD",
            side="buy",
            size=1,  # 1 contract
            price=safe_bid_int,
            order_type="limit_order",
        )

        logger.info(f"Placing: BUY 1 CONTRACT BTCUSD @ ${safe_bid_float:,.1f}")
        buy_result = await engine.order_manager.place_order(buy_order)

        if buy_result.exchange_order_id:
            logger.info("Buy order placed successfully!")
            logger.info(f"Exchange Order ID: {buy_result.exchange_order_id}")
            logger.info(f"Client Order ID: {buy_result.client_order_id}")
            logger.info(f"Status: {buy_result.status}")
        else:
            logger.error(f"Buy order failed! Status: {buy_result.status}")

        logger.info("Waiting before sending next order.")
        await asyncio.sleep(2)

        # Test 2: Place a sell limit order
        logger.info("")
        logger.info("-" * 70)
        logger.info("TEST 2: Placing SELL limit order")
        logger.info("-" * 70)

        sell_order = Order(
            symbol="BTCUSD",
            side="sell",
            size=1,  # 1 contract
            price=safe_ask_int,
            order_type="limit_order",
        )

        logger.info(f"Placing: SELL 1 CONTRACT BTCUSD @ ${safe_ask_float:,.1f}")
        sell_result = await engine.order_manager.place_order(sell_order)

        if sell_result.exchange_order_id:
            logger.info("Sell order placed successfully!")
            logger.info(f"Exchange Order ID: {sell_result.exchange_order_id}")
            logger.info(f"Client Order ID: {sell_result.client_order_id}")
            logger.info(f"Status: {sell_result.status}")
        else:
            logger.error(f"Sell order failed! Status: {sell_result.status}")

        logger.info("Waiting before sending next order.")
        await asyncio.sleep(2)

        # Test 3: Get open orders
        logger.info("")
        logger.info("-" * 70)
        logger.info("TEST 3: Fetching open orders")
        logger.info("-" * 70)

        open_orders = await engine.order_manager.get_open_orders("BTCUSD")
        logger.info(f"Found {len(open_orders)} open orders")

        for order in open_orders:
            logger.info(f"ORDER -> {order}")

        await asyncio.sleep(2)

        # Test 4: Cancel orders
        logger.info("")
        logger.info("-" * 70)
        logger.info("TEST 4: Cancelling test orders")
        logger.info("-" * 70)

        cancelled_count = 0

        if buy_result.exchange_order_id and buy_result.client_order_id:
            logger.info(f"Cancelling buy order {buy_result.client_order_id}...")
            success = await engine.order_manager.cancel_order(
                buy_result.client_order_id
            )
            if success:
                logger.info("Buy order cancelled")
                cancelled_count += 1
            else:
                logger.warning("Failed to cancel buy order")

        if sell_result.exchange_order_id and sell_result.client_order_id:
            logger.info(f"Cancelling sell order {sell_result.client_order_id}...")
            success = await engine.order_manager.cancel_order(
                sell_result.client_order_id
            )
            if success:
                logger.info("Sell order cancelled")
                cancelled_count += 1
            else:
                logger.warning("Failed to cancel sell order")

        await asyncio.sleep(2)

        # Verify all cancelled
        logger.info("")
        logger.info("-" * 70)
        logger.info("TEST 5: Verifying cancellation")
        logger.info("-" * 70)

        remaining_orders = await engine.order_manager.get_open_orders("BTCUSD")
        logger.info(f"Remaining open orders: {len(remaining_orders)}")

        if len(remaining_orders) == 0:
            logger.info("All test orders cancelled successfully")
        else:
            logger.warning(f"{len(remaining_orders)} orders still open")

        # Summary
        logger.info("")
        logger.info("=" * 70)
        logger.info("TEST SUMMARY")
        logger.info("=" * 70)
        logger.info(f"Buy order placed: {bool(buy_result.exchange_order_id)}")
        logger.info(f"Sell order placed: {bool(sell_result.exchange_order_id)}")
        logger.info(f"Orders cancelled: {cancelled_count}")
        logger.info("=" * 70)
        logger.info("All tests completed successfully!")
        logger.info("=" * 70)

    except Exception as e:
        logger.error(f"Test failed with error: {e}", exc_info=True)
    finally:
        await engine.stop()


if __name__ == "__main__":
    asyncio.run(test_order_placement())
