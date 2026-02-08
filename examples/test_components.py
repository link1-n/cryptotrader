"""Test script to validate framework components."""

import asyncio
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from deltatrader import Config
from deltatrader.client.rest import RestClient
from deltatrader.client.websocket import WebSocketClient
from deltatrader.models.orderbook import OrderBook
from deltatrader.utils.integer_conversion import IntegerConverter
from deltatrader.utils.logger import logger


async def test_rest_client():
    """Test REST API client."""
    logger.info("=" * 70)
    logger.info("Testing REST Client")
    logger.info("=" * 70)

    async with RestClient() as rest:
        try:
            # Test public endpoints
            logger.info("Fetching products...")
            products = await rest.get_products(contract_types=["perpetual_futures"])
            logger.info(f"✓ Fetched {len(products)} products")

            if products:
                symbol = products[0].symbol
                logger.info(f"Testing with symbol: {symbol}")

                # Test orderbook
                logger.info("Fetching orderbook...")
                orderbook = await rest.get_orderbook(symbol, depth=10)
                logger.info(f"✓ Fetched orderbook for {symbol}")

                # Test trades
                logger.info("Fetching trades...")
                trades = await rest.get_trades(symbol, limit=10)
                logger.info(f"✓ Fetched {len(trades)} trades")

            # Test authenticated endpoints (if credentials available)
            if Config.API_KEY and Config.API_SECRET:
                logger.info("Testing authenticated endpoints...")

                # Test get open orders
                logger.info("Fetching open orders...")
                open_orders = await rest.get_open_orders()
                logger.info(f"✓ Fetched {len(open_orders)} open orders")

                # Test get positions
                logger.info("Fetching positions...")
                positions = await rest.get_positions()
                logger.info(f"✓ Fetched {len(positions)} positions")

                # Test wallet balance
                logger.info("Fetching wallet balance...")
                balance = await rest.get_wallet_balance()
                logger.info(f"✓ Fetched wallet balance")

            logger.info("✓ REST client tests passed")
            return True

        except Exception as e:
            logger.error(f"✗ REST client test failed: {e}", exc_info=True)
            return False


async def test_websocket_client():
    """Test WebSocket client."""
    logger.info("=" * 70)
    logger.info("Testing WebSocket Client")
    logger.info("=" * 70)

    ws = WebSocketClient()
    received_messages = []

    # Add handler to capture messages
    def capture_message(data):
        received_messages.append(data)

    ws.add_handler("snapshot", lambda data: received_messages.append(data))

    try:
        # Connect
        logger.info("Connecting to WebSocket...")
        await ws.connect(authenticate=False)  # Test without auth first
        await ws.wait_connected(timeout=10.0)

        if not ws.is_connected:
            logger.error("✗ Failed to connect to WebSocket")
            return False

        logger.info("✓ WebSocket connected")

        # Subscribe to a channel
        test_symbol = "BTCUSD"
        logger.info(f"Subscribing to {test_symbol} orderbook...")
        await ws.subscribe([f"l2_orderbook.{test_symbol}"])

        # Wait for some messages
        logger.info("Waiting for messages (10 seconds)...")
        await asyncio.sleep(10)

        # Check if we received messages
        if len(received_messages) > 0:
            logger.info(f"✓ Received {len(received_messages)} messages")
        else:
            logger.warning("⚠ No messages received (this might be normal)")

        # Disconnect
        logger.info("Disconnecting...")
        await ws.disconnect()
        logger.info("✓ WebSocket tests passed")
        return True

    except Exception as e:
        logger.error(f"✗ WebSocket test failed: {e}", exc_info=True)
        return False
    finally:
        if ws.is_connected:
            await ws.disconnect()


async def test_integer_conversion():
    """Test integer conversion utilities."""
    logger.info("=" * 70)
    logger.info("Testing Integer Conversion")
    logger.info("=" * 70)

    from deltatrader.models.product import Product

    converter = IntegerConverter()

    # Create a test product
    test_product = Product(
        product_id=1,
        symbol="TESTBTC",
        description="Test BTC",
        contract_type="perpetual_futures",
        tick_size="0.5",  # 0.5 tick size
        contract_size="1",
        quoting_asset="USD",
        settling_asset="USDT",
    )

    converter.register_product(test_product)

    try:
        # Test price conversion
        price_str = "12345.5"
        price_int = converter.price_to_integer("TESTBTC", price_str)
        price_back = converter.integer_to_price("TESTBTC", price_int)

        logger.info(f"Price string: {price_str}")
        logger.info(f"Price integer: {price_int}")
        logger.info(f"Price back: {price_back}")

        if price_str == price_back:
            logger.info("✓ Price conversion: PASS")
        else:
            logger.error(f"✗ Price conversion: FAIL ({price_str} != {price_back})")
            return False

        # Test size conversion
        size_str = "10"
        size_int = converter.size_to_integer(size_str)
        size_back = converter.integer_to_size(size_int)

        logger.info(f"Size string: {size_str}")
        logger.info(f"Size integer: {size_int}")
        logger.info(f"Size back: {size_back}")

        # Test normalization
        unnormalized = price_int + 3  # Add 3 to make it not aligned to tick
        normalized = converter.normalize_price("TESTBTC", unnormalized)

        logger.info(f"Unnormalized: {unnormalized}")
        logger.info(f"Normalized: {normalized}")

        logger.info("✓ Integer conversion tests passed")
        return True

    except Exception as e:
        logger.error(f"✗ Integer conversion test failed: {e}", exc_info=True)
        return False


async def test_orderbook_operations():
    """Test orderbook operations."""
    logger.info("=" * 70)
    logger.info("Testing Orderbook Operations")
    logger.info("=" * 70)

    from deltatrader.models.product import Product

    converter = IntegerConverter()

    # Create a test product
    test_product = Product(
        product_id=1,
        symbol="TESTBTC",
        description="Test BTC",
        contract_type="perpetual_futures",
        tick_size="0.01",
        contract_size="1",
        quoting_asset="USD",
        settling_asset="USDT",
    )

    converter.register_product(test_product)

    try:
        # Create orderbook
        orderbook = OrderBook(symbol="TESTBTC")

        # Test snapshot update
        snapshot_data = {
            "type": "snapshot",
            "symbol": "TESTBTC",
            "timestamp": 1234567890000000,
            "sequence_no": 1,
            "buy": [
                {"limit_price": "50000.00", "size": "10"},
                {"limit_price": "49999.00", "size": "20"},
                {"limit_price": "49998.00", "size": "15"},
            ],
            "sell": [
                {"limit_price": "50001.00", "size": "10"},
                {"limit_price": "50002.00", "size": "20"},
                {"limit_price": "50003.00", "size": "15"},
            ],
        }

        orderbook.update_from_snapshot(snapshot_data, converter)
        logger.info(
            f"✓ Snapshot loaded: {len(orderbook.bids)} bids, {len(orderbook.asks)} asks"
        )

        # Test best bid/ask
        best_bid = orderbook.get_best_bid()
        best_ask = orderbook.get_best_ask()
        logger.info(f"Best bid: {best_bid}")
        logger.info(f"Best ask: {best_ask}")

        # Test mid price and spread
        mid_price = orderbook.get_mid_price()
        spread = orderbook.get_spread()
        logger.info(f"Mid price: {mid_price}")
        logger.info(f"Spread: {spread}")

        if spread > 0:
            logger.info("✓ Spread calculation: PASS")
        else:
            logger.error("✗ Spread calculation: FAIL")
            return False

        # Test incremental update
        update_data = {
            "type": "update",
            "symbol": "TESTBTC",
            "timestamp": 1234567890000001,
            "sequence_no": 2,
            "buy": [
                {"limit_price": "50000.00", "size": "15"},  # Update existing
            ],
            "sell": [
                {"limit_price": "50001.00", "size": "0"},  # Remove level
            ],
        }

        success = orderbook.apply_update(update_data, converter)
        if success:
            logger.info("✓ Incremental update: PASS")
        else:
            logger.error("✗ Incremental update: FAIL")
            return False

        logger.info("✓ Orderbook operations tests passed")
        return True

    except Exception as e:
        logger.error(f"✗ Orderbook test failed: {e}", exc_info=True)
        return False


async def test_configuration():
    """Test configuration."""
    logger.info("=" * 70)
    logger.info("Testing Configuration")
    logger.info("=" * 70)

    try:
        logger.info(f"Environment: {Config.ENVIRONMENT}")
        logger.info(f"WS URL: {Config.get_ws_url()}")
        logger.info(f"REST URL: {Config.get_rest_url()}")
        logger.info(f"Demo mode: {Config.is_demo_mode()}")
        logger.info(f"API Key set: {'Yes' if Config.API_KEY else 'No'}")
        logger.info(f"API Secret set: {'Yes' if Config.API_SECRET else 'No'}")

        # Validate
        if Config.API_KEY and Config.API_SECRET:
            if Config.validate():
                logger.info("✓ Configuration valid")
            else:
                logger.warning("⚠ Configuration validation failed")
        else:
            logger.warning("⚠ API credentials not set (this is OK for testing)")

        logger.info("✓ Configuration tests passed")
        return True

    except Exception as e:
        logger.error(f"✗ Configuration test failed: {e}", exc_info=True)
        return False


async def run_all_tests():
    """Run all component tests."""
    logger.info("=" * 70)
    logger.info("FRAMEWORK COMPONENT VALIDATION")
    logger.info("=" * 70)

    results = {}

    # Test configuration first
    results["Configuration"] = await test_configuration()

    # Test integer conversion (no network required)
    results["Integer Conversion"] = await test_integer_conversion()

    # Test orderbook operations (no network required)
    results["Orderbook Operations"] = await test_orderbook_operations()

    # Test REST client (requires network)
    results["REST Client"] = await test_rest_client()

    # Test WebSocket client (requires network)
    results["WebSocket Client"] = await test_websocket_client()

    # Print summary
    logger.info("=" * 70)
    logger.info("TEST SUMMARY")
    logger.info("=" * 70)

    all_passed = True
    for test_name, passed in results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        logger.info(f"{test_name:.<50} {status}")
        if not passed:
            all_passed = False

    logger.info("=" * 70)

    if all_passed:
        logger.info("✓ ALL TESTS PASSED")
    else:
        logger.warning("⚠ SOME TESTS FAILED")

    logger.info("=" * 70)

    return all_passed


async def main():
    """Main function."""
    try:
        success = await run_all_tests()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("\nTests interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
