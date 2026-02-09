"""
Demo script for l2_updates channel.

This example demonstrates:
- Subscribing to l2_updates channel for incremental order book updates
- Monitoring sequence numbers and checksums
- Handling multiple symbols efficiently
- Displaying real-time order book state

Configuration:
Set ORDERBOOK_CHANNEL=l2_updates in your .env file to use incremental updates.
"""

import asyncio
import os
import sys
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from deltatrader.client.rest import RestClient
from deltatrader.client.websocket import WebSocketClient
from deltatrader.core.market_data import MarketDataManager
from deltatrader.models.orderbook import OrderBook
from deltatrader.utils.config import Config
from deltatrader.utils.integer_conversion import IntegerConverter
from deltatrader.utils.logger import logger


class L2UpdatesMonitor:
    """Monitor and display l2_updates data."""

    def __init__(self):
        self.rest_client = RestClient()
        self.ws_client = WebSocketClient()
        self.converter = IntegerConverter()
        self.market_data = None

        # Statistics tracking
        self.snapshot_count = 0
        self.update_count = 0
        self.sequence_mismatches = 0
        self.checksum_failures = 0
        self.last_update_time = {}

    async def initialize(self, symbols: list[str]):
        """Initialize and connect clients."""
        logger.info(f"Using orderbook channel: {Config.ORDERBOOK_CHANNEL}")

        # Connect REST client and fetch products
        await self.rest_client.connect()
        products = await self.rest_client.get_products(
            contract_types=["perpetual_futures"]
        )

        # Register products with converter
        for product in products:
            self.converter.register_product(product)

        # Connect WebSocket
        await self.ws_client.connect(authenticate=False)

        # Create market data manager
        self.market_data = MarketDataManager(self.ws_client, self.converter)

        # Register callback
        self.market_data.add_orderbook_callback(self._on_orderbook_update)

        # Subscribe to symbols
        logger.info(f"Subscribing to {len(symbols)} symbols...")
        for symbol in symbols:
            await self.market_data.subscribe_orderbook(symbol)
            await asyncio.sleep(0.1)  # Small delay between subscriptions

        logger.info("✓ All subscriptions complete")

    async def _on_orderbook_update(self, symbol: str, orderbook: OrderBook):
        """Callback for orderbook updates."""
        now = datetime.now()
        self.last_update_time[symbol] = now

        # Track statistics
        if self.market_data._pending_snapshots.get(symbol, False):
            self.snapshot_count += 1
        else:
            self.update_count += 1

        # Display update
        best_bid = orderbook.get_best_bid()
        best_ask = orderbook.get_best_ask()
        spread = orderbook.get_spread()
        mid_price = orderbook.get_mid_price()

        # Convert to human-readable prices
        bid_price = self.converter.integer_to_price(symbol, best_bid[0])
        bid_size = self.converter.integer_to_size(best_bid[1])
        ask_price = self.converter.integer_to_price(symbol, best_ask[0])
        ask_size = self.converter.integer_to_size(best_ask[1])
        mid_price_str = self.converter.integer_to_price(symbol, mid_price)
        spread_str = self.converter.integer_to_price(symbol, spread)

        logger.info(
            f"{symbol:10s} | "
            f"Bid: {bid_price:>10s} ({bid_size:>6s}) | "
            f"Ask: {ask_price:>10s} ({ask_size:>6s}) | "
            f"Mid: {mid_price_str:>10s} | "
            f"Spread: {spread_str:>8s} | "
            f"Seq: {orderbook.sequence_no:>8d} | "
            f"Levels: {len(orderbook.bids)}/{len(orderbook.asks)}"
        )

    async def print_statistics(self):
        """Print periodic statistics."""
        while True:
            await asyncio.sleep(30)  # Every 30 seconds

            logger.info("")
            logger.info("=" * 80)
            logger.info("STATISTICS (Last 30s)")
            logger.info("=" * 80)
            logger.info(f"Channel: {Config.ORDERBOOK_CHANNEL}")
            logger.info(f"Snapshots received: {self.snapshot_count}")
            logger.info(f"Updates received: {self.update_count}")
            logger.info(f"Total messages: {self.snapshot_count + self.update_count}")
            logger.info(f"Sequence mismatches: {self.sequence_mismatches}")
            logger.info(f"Checksum failures: {self.checksum_failures}")

            if self.last_update_time:
                logger.info("")
                logger.info("Symbol updates:")
                for symbol, last_time in sorted(self.last_update_time.items()):
                    age = (datetime.now() - last_time).total_seconds()
                    orderbook = self.market_data.get_orderbook(symbol)
                    if orderbook:
                        logger.info(
                            f"  {symbol:10s}: seq={orderbook.sequence_no:>8d}, "
                            f"last_update={age:.1f}s ago"
                        )

            logger.info("=" * 80)
            logger.info("")

    async def run(self, symbols: list[str], duration: int = None):
        """Run the monitor."""
        await self.initialize(symbols)

        # Start statistics task
        stats_task = asyncio.create_task(self.print_statistics())

        try:
            if duration:
                logger.info(f"Running for {duration} seconds...")
                await asyncio.sleep(duration)
            else:
                logger.info("Running indefinitely (Ctrl+C to stop)...")
                await asyncio.Event().wait()
        except KeyboardInterrupt:
            logger.info("Stopped by user")
        finally:
            stats_task.cancel()
            await self.cleanup()

    async def cleanup(self):
        """Cleanup resources."""
        logger.info("Cleaning up...")

        if self.market_data:
            await self.market_data.cleanup()

        if self.ws_client:
            await self.ws_client.disconnect()

        if self.rest_client:
            await self.rest_client.close()

        logger.info("✓ Cleanup complete")


async def main():
    """Main entry point."""
    # Configure logging
    import logging

    logging.getLogger("deltatrader").setLevel(logging.INFO)

    # Display configuration
    print("\n" + "=" * 80)
    print("L2 Updates Channel Demo")
    print("=" * 80)
    print(f"Environment: {Config.ENVIRONMENT}")
    print(f"Orderbook Channel: {Config.ORDERBOOK_CHANNEL}")
    print(f"REST URL: {Config.get_rest_url()}")
    print(f"WebSocket URL: {Config.get_ws_url()}")
    print("=" * 80 + "\n")

    # Symbols to monitor
    symbols = [
        "BTCUSD",  # Bitcoin
        "ETHUSD",  # Ethereum
        "SOLUSD",  # Solana
    ]

    # Create and run monitor
    monitor = L2UpdatesMonitor()

    try:
        # Run for 5 minutes (or indefinitely if None)
        await monitor.run(symbols, duration=300)
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        await monitor.cleanup()


if __name__ == "__main__":
    # Usage examples:
    #
    # 1. Use l2_updates channel (default):
    #    ORDERBOOK_CHANNEL=l2_updates python examples/l2_updates_demo.py
    #
    # 2. Compare with l2_orderbook channel:
    #    ORDERBOOK_CHANNEL=l2_orderbook python examples/l2_updates_demo.py
    #
    # 3. Run on testnet:
    #    DELTA_ENVIRONMENT=testnet python examples/l2_updates_demo.py

    asyncio.run(main())
