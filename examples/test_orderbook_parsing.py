"""Test script to verify orderbook message parsing."""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from deltatrader.models.orderbook import OrderBook
from deltatrader.models.product import Product
from deltatrader.utils.integer_conversion import IntegerConverter
from deltatrader.utils.logger import logger

# Sample l2_orderbook message from Delta Exchange (based on your logs)
SAMPLE_L2_ORDERBOOK = {
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


def test_orderbook_parsing():
    """Test parsing of l2_orderbook messages."""
    logger.info("Testing orderbook message parsing...")
    logger.info("=" * 60)

    # Create a sample product
    product = Product(
        product_id=84,
        symbol="BTCUSD",
        description="Bitcoin Perpetual Futures",
        contract_type="perpetual_futures",
        tick_size="0.5",
        contract_size="1",
        quoting_asset="USD",
        settling_asset="USDT",
    )

    # Initialize converter
    converter = IntegerConverter()
    converter.register_product(product)

    # Create orderbook
    orderbook = OrderBook(symbol="BTCUSD")

    # Parse the message
    try:
        orderbook.update_from_snapshot(SAMPLE_L2_ORDERBOOK, converter)
        logger.info("✓ Successfully parsed l2_orderbook message")
    except Exception as e:
        logger.error(f"✗ Failed to parse message: {e}")
        import traceback

        traceback.print_exc()
        return False

    # Verify the data
    logger.info(f"Symbol: {orderbook.symbol}")
    logger.info(f"Timestamp: {orderbook.timestamp}")
    logger.info(f"Sequence: {orderbook.sequence_no}")
    logger.info(f"Bids: {len(orderbook.bids)}")
    logger.info(f"Asks: {len(orderbook.asks)}")

    # Check best bid/ask
    best_bid = orderbook.get_best_bid()
    best_ask = orderbook.get_best_ask()
    mid_price = orderbook.get_mid_price()
    spread = orderbook.get_spread()

    logger.info("")
    logger.info("Market Data:")
    logger.info(f"  Best Bid: {best_bid[0]} (size: {best_bid[1]})")
    logger.info(f"  Best Ask: {best_ask[0]} (size: {best_ask[1]})")
    logger.info(f"  Mid Price: {mid_price}")
    logger.info(f"  Spread: {spread}")

    # Convert back to float for display
    best_bid_price = float(converter.integer_to_price("BTCUSD", best_bid[0]))
    best_ask_price = float(converter.integer_to_price("BTCUSD", best_ask[0]))
    mid_price_float = float(converter.integer_to_price("BTCUSD", mid_price))
    spread_float = float(converter.integer_to_price("BTCUSD", spread))

    logger.info("")
    logger.info("Market Data (as float):")
    logger.info(f"  Best Bid: ${best_bid_price:,.1f}")
    logger.info(f"  Best Ask: ${best_ask_price:,.1f}")
    logger.info(f"  Mid Price: ${mid_price_float:,.1f}")
    logger.info(f"  Spread: ${spread_float:,.1f}")

    # Verify values match expected
    logger.info("")
    logger.info("Validation:")

    if best_bid[0] > 0 and best_ask[0] > 0:
        logger.info("✓ Best bid and ask are populated")
    else:
        logger.error("✗ Best bid or ask is zero!")
        return False

    if best_bid[0] < best_ask[0]:
        logger.info("✓ Bid is less than ask (correct)")
    else:
        logger.error("✗ Bid is greater than ask (incorrect!)")
        return False

    if spread > 0:
        logger.info("✓ Spread is positive")
    else:
        logger.error("✗ Spread is not positive!")
        return False

    if orderbook.sequence_no == 1088568:
        logger.info("✓ Sequence number parsed correctly")
    else:
        logger.error(f"✗ Sequence number incorrect: {orderbook.sequence_no} != 1088568")
        return False

    # Show top 3 levels
    logger.info("")
    logger.info("Top 3 Bid Levels:")
    for i, (price, size) in enumerate(orderbook.bids[:3]):
        price_float = float(converter.integer_to_price("BTCUSD", price))
        size_float = float(converter.integer_to_size(size))
        logger.info(f"  {i + 1}. ${price_float:,.1f} x {size_float}")

    logger.info("")
    logger.info("Top 3 Ask Levels:")
    for i, (price, size) in enumerate(orderbook.asks[:3]):
        price_float = float(converter.integer_to_price("BTCUSD", price))
        size_float = float(converter.integer_to_size(size))
        logger.info(f"  {i + 1}. ${price_float:,.1f} x {size_float}")

    logger.info("")
    logger.info("=" * 60)
    logger.info("✓ All tests passed!")
    return True


if __name__ == "__main__":
    success = test_orderbook_parsing()
    sys.exit(0 if success else 1)
