"""Market data manager for orderbooks and trades."""

import asyncio
from collections.abc import Callable

from ..client.websocket import WebSocketClient
from ..models.orderbook import OrderBook
from ..models.trade import Trade
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

        Args:
            symbol: Trading symbol (e.g., "BTCUSD")
        """
        # Initialize orderbook
        if symbol not in self._orderbooks:
            self._orderbooks[symbol] = OrderBook(symbol=symbol)
            self._pending_snapshots[symbol] = True

        # Subscribe to l2_updates channel
        channel = f"l2_orderbook.{symbol}"
        await self.ws_client.subscribe([channel])

        # Add handler for this symbol
        self.ws_client.add_handler(channel, self._handle_orderbook_message)

        logger.info(f"Subscribed to orderbook: {symbol}")

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
        channel = f"l2_orderbook.{symbol}"
        await self.ws_client.unsubscribe([channel])

        async with self._lock:
            if symbol in self._orderbooks:
                del self._orderbooks[symbol]

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
        """Handle orderbook update message."""
        try:
            msg_type = data.get("type")
            symbol = data.get("symbol")

            if not symbol:
                logger.warning("Orderbook message missing symbol")
                return

            async with self._lock:
                orderbook = self._orderbooks.get(symbol)
                if not orderbook:
                    orderbook = OrderBook(symbol=symbol)
                    self._orderbooks[symbol] = orderbook

                # Handle l2_orderbook (full snapshot from Delta Exchange)
                if msg_type == "l2_orderbook":
                    print(data)
                    orderbook.update_from_snapshot(data, self.converter)
                    self._pending_snapshots[symbol] = False
                    best_bid = orderbook.get_best_bid()
                    best_ask = orderbook.get_best_ask()
                    logger.info(
                        f"Orderbook snapshot: {symbol} - "
                        f"bid={best_bid[0]}/{best_bid[1]}, "
                        f"ask={best_ask[0]}/{best_ask[1]}, "
                        f"seq={orderbook.sequence_no}, "
                        f"bids={len(orderbook.bids)}, asks={len(orderbook.asks)}"
                    )

                # Handle snapshot (incremental snapshot format)
                elif msg_type == "snapshot":
                    orderbook.update_from_snapshot(data, self.converter)
                    self._pending_snapshots[symbol] = False
                    best_bid = orderbook.get_best_bid()
                    best_ask = orderbook.get_best_ask()
                    logger.info(
                        f"Orderbook snapshot: {symbol} - "
                        f"bid={best_bid[0]}/{best_bid[1]}, "
                        f"ask={best_ask[0]}/{best_ask[1]}, "
                        f"seq={orderbook.sequence_no}"
                    )

                # Handle incremental update
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
                        await self.ws_client.unsubscribe([f"l2_orderbook.{symbol}"])
                        await asyncio.sleep(0.1)
                        await self.ws_client.subscribe([f"l2_orderbook.{symbol}"])
                        return

                # Validate checksum if provided
                checksum = data.get("cs")
                if checksum and not orderbook.validate_checksum(
                    checksum, self.converter
                ):
                    logger.warning(f"Checksum validation failed for {symbol}")

            # Notify callbacks
            await self._notify_orderbook_callbacks(symbol, orderbook)

        except Exception as e:
            logger.error(f"Error handling orderbook message: {e}", exc_info=True)

    async def _handle_trade_message(self, data: dict) -> None:
        """Handle trade update message."""
        try:
            msg_type = data.get("type")
            symbol = data.get("symbol")
            trades_data = data.get("trades", [])

            if not symbol:
                logger.warning("Trade message missing symbol")
                return

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

                if msg_type == "all_trades_snapshot":
                    logger.debug(
                        f"Trades snapshot received: {symbol} ({len(new_trades)} trades)"
                    )
                elif msg_type == "all_trades":
                    logger.debug(
                        f"Trade update received: {symbol} ({len(new_trades)} trades)"
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
