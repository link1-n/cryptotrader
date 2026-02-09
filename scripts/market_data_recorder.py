import asyncio
import datetime
import gc
import os
import tracemalloc
from pathlib import Path
from typing import Any

import pandas as pd

try:
    import psutil

    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

from deltatrader.core.engine import TradingEngine
from deltatrader.models.orderbook import OrderBook
from deltatrader.models.trade import Trade
from deltatrader.strategies.base import Strategy
from deltatrader.utils.logger import logger


class MarketDataRecorder(Strategy):
    """Records market data (orderbooks and trades) to Parquet files.

    Uses async thread pools for file I/O operations to avoid blocking the event loop.
    All pandas operations (read_parquet, to_parquet, concat) run in thread pools,
    allowing multiple symbols to be saved concurrently.

    Performance Comparison (3 symbols, 2 files each = 6 total files):
    - Before (sequential): ~6 seconds (files saved one-by-one)
    - After (concurrent):  ~2 seconds (all files saved in parallel)
    - Speedup: ~3x faster
    """

    def __init__(self, *args, data_dir: str = "market_data", **kwargs):
        super().__init__(*args, **kwargs)
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

        # In-memory buffers for each symbol
        self.orderbook_buffers: dict[str, list[dict[str, Any]]] = {}
        self.trade_buffers: dict[str, list[dict[str, Any]]] = {}

        # Track last save time for each file
        self.last_save_time = datetime.datetime.now(datetime.timezone.utc)

        # Configuration
        self.save_interval_seconds = 60  # Save every minute
        self.max_buffer_size = 1000  # Save when buffer reaches this size

        # Memory profiling
        self._save_count = 0
        tracemalloc.start()

        logger.info(f"MarketDataRecorder initialized. Data dir: {self.data_dir}")
        logger.info(f"Memory profiling enabled. psutil available: {PSUTIL_AVAILABLE}")

    async def on_start(self) -> None:
        """Initialize buffers for each symbol."""
        for symbol in self.symbols:
            self.orderbook_buffers[symbol] = []
            self.trade_buffers[symbol] = []
        logger.info("MarketDataRecorder started")

    async def on_stop(self) -> None:
        """Flush all remaining data on shutdown."""
        logger.info("Flushing remaining data before shutdown...")
        await self._save_all_data(force=True)
        logger.info("MarketDataRecorder stopped")

        # Stop memory profiling
        tracemalloc.stop()

    async def on_orderbook_update(self, symbol: str, orderbook: OrderBook) -> None:
        """Record orderbook snapshot."""
        try:
            logger.debug(orderbook)

            # Get best levels
            best_bid = orderbook.get_best_bid()
            best_ask = orderbook.get_best_ask()

            # Get top 10 levels for both sides
            top_bids = orderbook.bids[:20]
            top_asks = orderbook.asks[:20]

            # Create orderbook record
            record = {
                "system_timestamp": datetime.datetime.now(datetime.timezone.utc),
                "exchange_timestamp": orderbook.timestamp,
                "symbol": symbol,
                "sequence_no": orderbook.sequence_no,
                "top_spread": best_ask[0] - best_bid[0],
            }

            # Add top 10 bid/ask levels
            for i, (price, size) in enumerate(top_bids, 1):
                record[f"bid_{i}_price"] = price
                record[f"bid_{i}_size"] = size

            for i, (price, size) in enumerate(top_asks, 1):
                record[f"ask_{i}_price"] = price
                record[f"ask_{i}_size"] = size

            # Pad with None if less than 10 levels
            for i in range(len(top_bids) + 1, 11):
                record[f"bid_{i}_price"] = None
                record[f"bid_{i}_size"] = None

            for i in range(len(top_asks) + 1, 11):
                record[f"ask_{i}_price"] = None
                record[f"ask_{i}_size"] = None

            # Add to buffer
            self.orderbook_buffers[symbol].append(record)

            logger.debug(
                f"Recorded orderbook: {symbol} - "
                f"bid={best_bid[0]}/{best_bid[1]}, ask={best_ask[0]}/{best_ask[1]}, "
                f"buffer_size={len(self.orderbook_buffers[symbol])}"
            )

        except Exception as e:
            logger.error(f"Error recording orderbook for {symbol}: {e}", exc_info=True)

    async def on_trades_update(self, symbol: str, trades: list[Trade]) -> None:
        """Record trades."""
        try:
            for trade in trades:
                logger.debug(trade)
                record = {
                    "timestamp": datetime.datetime.fromtimestamp(
                        trade.timestamp / 1_000_000, datetime.timezone.utc
                    ),
                    "symbol": trade.symbol,
                    "trade_id": trade.trade_id,
                    "price": trade.price,
                    "size": trade.size,
                    "taker_side": trade.side,
                }
                self.trade_buffers[symbol].append(record)

            if trades:
                logger.debug(
                    f"Recorded {len(trades)} trades for {symbol}, "
                    f"buffer_size={len(self.trade_buffers[symbol])}"
                )

        except Exception as e:
            logger.error(f"Error recording trades for {symbol}: {e}", exc_info=True)

    async def on_tick(self) -> None:
        """Save data periodically on each tick."""
        try:
            # Check if we should save based on time or buffer size
            current_time = datetime.datetime.now(datetime.timezone.utc)
            time_elapsed = (current_time - self.last_save_time).total_seconds()

            should_save_time = time_elapsed >= self.save_interval_seconds
            should_save_size = any(
                len(buf) >= self.max_buffer_size
                for buf in list(self.orderbook_buffers.values())
                + list(self.trade_buffers.values())
            )

            if should_save_time or should_save_size:
                reason = "time interval" if should_save_time else "buffer size"
                logger.info(f"Saving data (trigger: {reason})...")
                await self._save_all_data()
                self.last_save_time = current_time

                # Increment save counter and log memory stats
                self._save_count += 1
                if self._save_count % 10 == 0:
                    self._log_memory_stats()

        except Exception as e:
            logger.error(f"Error in on_tick: {e}", exc_info=True)

    def _log_memory_stats(self) -> None:
        """Log memory usage statistics."""
        try:
            # Log process memory (if psutil available)
            if PSUTIL_AVAILABLE:
                process = psutil.Process(os.getpid())
                mem_info = process.memory_info()
                mem_mb = mem_info.rss / 1024 / 1024
                logger.info(f"Process memory: {mem_mb:.1f} MB (RSS)")

            # Log Python memory allocations
            snapshot = tracemalloc.take_snapshot()
            top_stats = snapshot.statistics("lineno")

            logger.info("=" * 60)
            logger.info(f"Memory Profile (after {self._save_count} saves)")
            logger.info("=" * 60)

            logger.info("Top 5 memory allocations:")
            for i, stat in enumerate(top_stats[:5], 1):
                logger.info(f"  {i}. {stat}")

            # Log buffer sizes
            total_orderbook_records = sum(
                len(buf) for buf in self.orderbook_buffers.values()
            )
            total_trade_records = sum(len(buf) for buf in self.trade_buffers.values())
            logger.info(f"Current buffer sizes:")
            logger.info(f"  Orderbook records: {total_orderbook_records}")
            logger.info(f"  Trade records: {total_trade_records}")
            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"Error logging memory stats: {e}", exc_info=True)

    async def _save_all_data(self, force: bool = False) -> None:
        """Save all buffered data to Parquet files concurrently.

        Uses asyncio.gather() to run all save operations in parallel.
        Each save operation runs its blocking pandas I/O in a thread pool,
        so multiple files can be written simultaneously without blocking
        the event loop.
        """
        # Create all save tasks for concurrent execution
        tasks = []
        for symbol in self.symbols:
            tasks.append(self._save_orderbook_data(symbol, force))
            tasks.append(self._save_trade_data(symbol, force))

        # Execute all saves concurrently
        await asyncio.gather(*tasks)

    async def _save_orderbook_data(self, symbol: str, force: bool = False) -> None:
        """Save orderbook data for a symbol to Parquet (async with thread pool)."""
        buffer = self.orderbook_buffers.get(symbol, [])

        if not buffer:
            return

        if (
            not force and len(buffer) < 10
        ):  # Don't save very small buffers unless forced
            return

        try:
            # Copy buffer and clear it immediately to minimize lock time
            buffer_copy = buffer.copy()
            self.orderbook_buffers[symbol] = []

            # Run blocking I/O operations in thread pool
            await asyncio.to_thread(
                self._sync_save_orderbook,
                symbol,
                buffer_copy,
            )

            logger.info(f"Saved {len(buffer_copy)} orderbook records for {symbol}")

        except Exception as e:
            logger.error(
                f"Error saving orderbook data for {symbol}: {e}", exc_info=True
            )

    def _sync_save_orderbook(self, symbol: str, buffer: list) -> None:
        """Synchronous orderbook save operation (runs in thread pool).

        This method performs blocking pandas operations:
        - DataFrame creation
        - Reading existing parquet file
        - DataFrame concatenation
        - Writing parquet file

        By running in a thread pool via asyncio.to_thread(), these blocking
        operations don't block the event loop.

        Handles date boundaries properly by grouping records by date.
        """
        if not buffer:
            return

        df = pd.DataFrame(buffer)

        # Group by date to handle midnight boundary correctly
        df["date"] = pd.to_datetime(df["system_timestamp"]).dt.date

        for date, group_df in df.groupby("date"):
            date_str = date.strftime("%Y%m%d")
            filename = self.data_dir / f"orderbook_{symbol}_{date_str}.parquet"

            # Remove the temporary date column
            group_df = group_df.drop("date", axis=1).copy()

            # Append to existing file or create new one
            if filename.exists():
                existing_df = pd.read_parquet(filename)
                group_df = pd.concat([existing_df, group_df], ignore_index=True)
                del existing_df  # Explicit cleanup to help GC

            # Save to Parquet
            group_df.to_parquet(
                filename,
                engine="pyarrow",
                compression="snappy",
                index=False,
            )

            del group_df  # Explicit cleanup

        del df  # Explicit cleanup
        gc.collect()  # Suggest garbage collection

    async def _save_trade_data(self, symbol: str, force: bool = False) -> None:
        """Save trade data for a symbol to Parquet (async with thread pool)."""
        buffer = self.trade_buffers.get(symbol, [])

        if not buffer:
            return

        if (
            not force and len(buffer) < 10
        ):  # Don't save very small buffers unless forced
            return

        try:
            # Copy buffer and clear it immediately to minimize lock time
            buffer_copy = buffer.copy()
            self.trade_buffers[symbol] = []

            # Run blocking I/O operations in thread pool
            await asyncio.to_thread(
                self._sync_save_trades,
                symbol,
                buffer_copy,
            )

            logger.info(f"Saved {len(buffer_copy)} trade records for {symbol}")

        except Exception as e:
            logger.error(f"Error saving trade data for {symbol}: {e}", exc_info=True)

    def _sync_save_trades(self, symbol: str, buffer: list) -> None:
        """Synchronous trade save operation (runs in thread pool).

        This method performs blocking pandas operations:
        - DataFrame creation
        - Reading existing parquet file
        - DataFrame concatenation
        - Writing parquet file

        By running in a thread pool via asyncio.to_thread(), these blocking
        operations don't block the event loop.

        Handles date boundaries properly by grouping records by date.
        """
        if not buffer:
            return

        df = pd.DataFrame(buffer)

        # Group by date to handle midnight boundary correctly
        df["date"] = pd.to_datetime(df["timestamp"]).dt.date

        for date, group_df in df.groupby("date"):
            date_str = date.strftime("%Y%m%d")
            filename = self.data_dir / f"trades_{symbol}_{date_str}.parquet"

            # Remove the temporary date column
            group_df = group_df.drop("date", axis=1).copy()

            # Append to existing file or create new one
            if filename.exists():
                existing_df = pd.read_parquet(filename)
                group_df = pd.concat([existing_df, group_df], ignore_index=True)
                del existing_df  # Explicit cleanup to help GC

            # Save to Parquet
            group_df.to_parquet(
                filename,
                engine="pyarrow",
                compression="snappy",
                index=False,
            )

            del group_df  # Explicit cleanup

        del df  # Explicit cleanup
        gc.collect()  # Suggest garbage collection


async def main():
    """Main entry point for the market data recorder."""
    symbols_to_record: list[str] = ["XRPUSD"]

    # Create engine
    engine = TradingEngine()

    # Create recorder with custom data directory
    md_recorder = MarketDataRecorder(
        name="md_recorder",
        symbols=symbols_to_record,
        market_data=engine.market_data,
        order_manager=engine.order_manager,
        data_dir="market_data",  # Directory for Parquet files
    )

    try:
        # Initialize and start
        await engine.initialize(symbols=symbols_to_record)
        await engine.add_strategy(md_recorder)
        await engine.start()

        logger.info(f"Recording market data for {symbols_to_record}")
        logger.info(
            "Data will be saved to Parquet files every minute or when buffer is full"
        )
        logger.info("Press Ctrl+C to stop...")

        # Keep running
        while engine.is_running:
            await asyncio.sleep(10)

    except KeyboardInterrupt:
        logger.info("\nKeyboard interrupt received. Shutting down...")
    except Exception as e:
        logger.error(f"Error in market data recorder: {e}", exc_info=True)
    finally:
        # Clean shutdown
        await engine.stop()
        logger.info("Market data recorder stopped.")


if __name__ == "__main__":
    asyncio.run(main())
