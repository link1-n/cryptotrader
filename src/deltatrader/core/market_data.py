"""Market data manager for orderbooks and trades."""

import asyncio
from collections.abc import Callable

from ..client.websocket import WebSocketClient
from ..models.orderbook import OrderBook
from ..models.trade import Trade
from ..utils.config import Config
from ..utils.integer_conversion import IntegerConverter
from ..utils.logger import logger


class MarketDataManager:
    """Manages real-time market data (orderbooks and trades) for multiple symbols."""

    def __init__(self, ws_client: WebSocketClient, converter: IntegerConverter):
        """
        Initialize MarketDataManager.

        Args:
            ws_client: WebSocket client instance
            converter: Integer converter instance
        """
        self.ws_client = ws_client
        self.converter = converter

        # Market data storage
        self._orderbooks: dict[str, OrderBook] = {}
        self._trades: dict[str, list[Trade]] = {}
        self._max_trades_per_symbol = 100

        # Callbacks for updates
        self._orderbook_callbacks: list[Callable] = []
        self._trade_callbacks: list[Callable] = []

        # Lock for thread-safe access
        self._lock = asyncio.Lock()

        # Track pending snapshot requests
        self._pending_snapshots: dict[str, bool] = {}

    async def subscribe_orderbook(self, symbol: str) -> None:
        """
        Subscribe to orderbook updates for a symbol.

        Uses the channel configured in Config.ORDERBOOK_CHANNEL:
        - l2_orderbook: Full L2 snapshots sent periodically
        - l2_updates: Initial snapshot + incremental updates

        Args:
            symbol: Trading symbol (e.g., "BTCUSD")
        """
        # Initialize orderbook
        if symbol not in self._orderbooks:
            self._orderbooks[symbol] = OrderBook(symbol=symbol)
            self._pending_snapshots[symbol] = True

        # Subscribe to configured orderbook channel
        channel_type = Config.ORDERBOOK_CHANNEL
        channel = f"{channel_type}.{symbol}"
        await self.ws_client.subscribe([channel])

        # Add handler for this symbol
        self.ws_client.add_handler(channel, self._handle_orderbook_message)

        logger.info(f"Subscribed to orderbook ({channel_type}): {symbol}")

    async def subscribe_trades(self, symbol: str) -> None:
        """
        Subscribe to trade updates for a symbol.

        Args:
            symbol: Trading symbol
        """
        # Initialize trades list
        if symbol not in self._trades:
            self._trades[symbol] = []

        # Subscribe to all_trades channel
        channel = f"all_trades.{symbol}"
        await self.ws_client.subscribe([channel])

        # Add handler for this symbol
        self.ws_client.add_handler(channel, self._handle_trade_message)

        logger.info(f"Subscribed to trades: {symbol}")

    async def unsubscribe_orderbook(self, symbol: str) -> None:
        """
        Unsubscribe from orderbook updates for a symbol.

        Args:
            symbol: Trading symbol
        """
        channel_type = Config.ORDERBOOK_CHANNEL
        channel = f"{channel_type}.{symbol}"
        await self.ws_client.unsubscribe([channel])

        async with self._lock:
            if symbol in self._orderbooks:
                del self._orderbooks[symbol]
            if symbol in self._pending_snapshots:
                del self._pending_snapshots[symbol]

        logger.info(f"Unsubscribed from orderbook: {symbol}")

    async def unsubscribe_trades(self, symbol: str) -> None:
        """
        Unsubscribe from trade updates for a symbol.

        Args:
            symbol: Trading symbol
        """
        channel = f"all_trades.{symbol}"
        await self.ws_client.unsubscribe([channel])

        async with self._lock:
            if symbol in self._trades:
                del self._trades[symbol]

        logger.info(f"Unsubscribed from trades: {symbol}")

    async def _handle_orderbook_message(self, data: dict) -> None:
        """
        Handle orderbook update message.

        Supports both l2_orderbook and l2_updates channel formats:
        - l2_orderbook: Full snapshots with type="l2_orderbook"
        - l2_updates: Initial snapshot (action="snapshot") + incremental updates (action="update")
        """
        try:
            logger.debug(f"ORDERBOOKMSG: {data}")
            # l2_updates messages have BOTH type="l2_updates" AND action="snapshot"/"update"
            # We need to prioritize the action field for l2_updates messages
            action = data.get("action")
            msg_type = action if action else data.get("type")
            symbol = data.get("symbol")

            if not symbol:
                logger.warning("Orderbook message missing symbol")
                return

            # Handle error messages
            if msg_type == "error":
                error_msg = data.get("message", "Unknown error")
                logger.error(f"Orderbook error for {symbol}: {error_msg}")
                return

            async with self._lock:
                orderbook = self._orderbooks.get(symbol)
                if not orderbook:
                    orderbook = OrderBook(symbol=symbol)
                    self._orderbooks[symbol] = orderbook

                # Handle l2_orderbook (full snapshot from l2_orderbook channel)
                if msg_type == "l2_orderbook":
                    orderbook.update_from_snapshot(data, self.converter)
                    self._pending_snapshots[symbol] = False
                    best_bid = orderbook.get_best_bid()
                    best_ask = orderbook.get_best_ask()
                    logger.info(
                        f"Orderbook snapshot (l2_orderbook): {symbol} - "
                        f"bid={best_bid[0]}/{best_bid[1]}, "
                        f"ask={best_ask[0]}/{best_ask[1]}, "
                        f"seq={orderbook.sequence_no}, "
                        f"bids={len(orderbook.bids)}, asks={len(orderbook.asks)}"
                    )

                # Handle snapshot (from l2_updates channel or legacy format)
                elif msg_type == "snapshot":
                    orderbook.update_from_snapshot(data, self.converter)
                    self._pending_snapshots[symbol] = False
                    best_bid = orderbook.get_best_bid()
                    best_ask = orderbook.get_best_ask()
                    logger.info(
                        f"Orderbook snapshot (l2_updates): {symbol} - "
                        f"bid={best_bid[0]}/{best_bid[1]}, "
                        f"ask={best_ask[0]}/{best_ask[1]}, "
                        f"seq={orderbook.sequence_no}, "
                        f"bids={len(orderbook.bids)}, asks={len(orderbook.asks)}"
                    )

                # Handle incremental update (from l2_updates channel)
                elif msg_type == "update":
                    # Skip updates until we have a snapshot
                    if self._pending_snapshots.get(symbol, True):
                        logger.debug(f"Skipping update, waiting for snapshot: {symbol}")
                        return

                    # Apply update
                    success = orderbook.apply_update(data, self.converter)

                    if not success:
                        # Sequence mismatch, need to resubscribe
                        logger.warning(
                            f"Sequence mismatch for {symbol}, resubscribing for snapshot"
                        )
                        self._pending_snapshots[symbol] = True
                        channel_type = Config.ORDERBOOK_CHANNEL
                        channel = f"{channel_type}.{symbol}"
                        await self.ws_client.unsubscribe([channel])
                        await asyncio.sleep(0.1)
                        await self.ws_client.subscribe([channel])
                        return

                    logger.debug(
                        f"Applied orderbook update: {symbol} seq={orderbook.sequence_no}"
                    )

                else:
                    logger.warning(f"Unknown orderbook message type: {msg_type}")
                    return

                # Validate checksum if provided
                checksum = data.get("cs")
                if checksum:
                    computed = orderbook.compute_checksum(self.converter)
                    if computed != checksum:
                        # Build checksum string for debugging
                        top_raw_asks = orderbook._raw_asks[:10]
                        top_raw_bids = orderbook._raw_bids[:10]
                        ask_parts = [f"{price}:{size}" for price, size in top_raw_asks]
                        bid_parts = [f"{price}:{size}" for price, size in top_raw_bids]
                        checksum_string = (
                            ",".join(ask_parts) + "|" + ",".join(bid_parts)
                        )

                        logger.warning(
                            f"Checksum validation failed for {symbol} "
                            f"(expected={checksum}, computed={computed})\n"
                            f"Checksum string: {checksum_string[:200]}..."
                            if len(checksum_string) > 200
                            else f"Checksum string: {checksum_string}"
                        )
                        # Optionally resubscribe on checksum failure
                        # self._pending_snapshots[symbol] = True
                        # await self._resubscribe_orderbook(symbol)

            # Notify callbacks
            await self._notify_orderbook_callbacks(symbol, orderbook)

        except Exception as e:
            logger.error(f"Error handling orderbook message: {e}", exc_info=True)

    async def _handle_trade_message(self, data: dict) -> None:
        """Handle trade update message."""
        try:
            logger.debug(f"TRADEMSG: {data}")
            msg_type = data.get("type")
            symbol = data.get("symbol")

            if not symbol:
                logger.warning("Trade message missing symbol")
                return

            # Delta Exchange sends trade data in two formats:
            # 1. Array format: {"type": "all_trades_snapshot", "trades": [...]}
            # 2. Single trade: {"type": "all_trades", "price": "...", "size": ..., ...}
            trades_data = data.get("trades")
            if trades_data is None:
                # Single trade message - the data dict itself is the trade
                trades_data = [data]

            async with self._lock:
                if symbol not in self._trades:
                    self._trades[symbol] = []

                # Parse and store trades
                new_trades = []
                for trade_data in trades_data:
                    try:
                        trade = Trade.from_api(symbol, trade_data, self.converter)
                        new_trades.append(trade)
                    except Exception as e:
                        logger.warning(f"Failed to parse trade: {e}")

                # Add new trades and maintain max size
                self._trades[symbol].extend(new_trades)
                if len(self._trades[symbol]) > self._max_trades_per_symbol:
                    self._trades[symbol] = self._trades[symbol][
                        -self._max_trades_per_symbol :
                    ]

                logger.debug(
                    f"Trades received: {symbol} ({len(new_trades)} trades) ({msg_type} type)"
                )

            # Notify callbacks
            await self._notify_trade_callbacks(symbol, new_trades)

        except Exception as e:
            logger.error(f"Error handling trade message: {e}", exc_info=True)

    def get_orderbook(self, symbol: str) -> OrderBook | None:
        """
        Get current orderbook for a symbol (non-async, returns copy).

        Args:
            symbol: Trading symbol

        Returns:
            OrderBook or None if not available
        """
        return self._orderbooks.get(symbol)

    def get_trades(self, symbol: str, limit: int | None = None) -> list[Trade]:
        """
        Get recent trades for a symbol.

        Args:
            symbol: Trading symbol
            limit: Maximum number of trades to return

        Returns:
            List of Trade objects
        """
        trades = self._trades.get(symbol, [])
        if limit:
            return trades[-limit:]
        return trades.copy()

    def get_best_bid(self, symbol: str) -> int | None:
        """
        Get best bid price as integer.

        Args:
            symbol: Trading symbol

        Returns:
            Best bid price or None
        """
        orderbook = self._orderbooks.get(symbol)
        if orderbook and orderbook.bids:
            return orderbook.bids[0][0]
        return None

    def get_best_ask(self, symbol: str) -> int | None:
        """
        Get best ask price as integer.

        Args:
            symbol: Trading symbol

        Returns:
            Best ask price or None
        """
        orderbook = self._orderbooks.get(symbol)
        if orderbook and orderbook.asks:
            return orderbook.asks[0][0]
        return None

    def get_mid_price(self, symbol: str) -> int | None:
        """
        Get mid price as integer.

        Args:
            symbol: Trading symbol

        Returns:
            Mid price or None
        """
        orderbook = self._orderbooks.get(symbol)
        if orderbook:
            return orderbook.get_mid_price()
        return None

    def get_spread(self, symbol: str) -> int | None:
        """
        Get spread as integer.

        Args:
            symbol: Trading symbol

        Returns:
            Spread or None
        """
        orderbook = self._orderbooks.get(symbol)
        if orderbook:
            return orderbook.get_spread()
        return None

    def add_orderbook_callback(self, callback: Callable) -> None:
        """
        Add a callback for orderbook updates.

        Callback signature: async def callback(symbol: str, orderbook: OrderBook)

        Args:
            callback: Async callback function
        """
        self._orderbook_callbacks.append(callback)

    def add_trade_callback(self, callback: Callable) -> None:
        """
        Add a callback for trade updates.

        Callback signature: async def callback(symbol: str, trades: list[Trade])

        Args:
            callback: Async callback function
        """
        self._trade_callbacks.append(callback)

    async def _notify_orderbook_callbacks(
        self, symbol: str, orderbook: OrderBook
    ) -> None:
        """Notify all orderbook callbacks."""
        for callback in self._orderbook_callbacks:
            try:
                asyncio.create_task(callback(symbol, orderbook))
            except Exception as e:
                logger.error(f"Error in orderbook callback: {e}")

    async def _notify_trade_callbacks(self, symbol: str, trades: list[Trade]) -> None:
        """Notify all trade callbacks."""
        for callback in self._trade_callbacks:
            try:
                asyncio.create_task(callback(symbol, trades))
            except Exception as e:
                logger.error(f"Error in trade callback: {e}")

    def get_subscribed_symbols(self) -> list[str]:
        """Get list of all subscribed symbols."""
        symbols = set()
        symbols.update(self._orderbooks.keys())
        symbols.update(self._trades.keys())
        return list(symbols)

    async def cleanup(self) -> None:
        """Clean up resources."""
        async with self._lock:
            self._orderbooks.clear()
            self._trades.clear()
            self._orderbook_callbacks.clear()
            self._trade_callbacks.clear()
        logger.info("Market data manager cleaned up")
