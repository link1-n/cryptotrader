"""Base strategy class for implementing trading strategies."""

from abc import ABC, abstractmethod

from ..core.market_data import MarketDataManager
from ..core.order_manager import OrderManager
from ..models.order import Order
from ..models.orderbook import OrderBook
from ..models.trade import Trade
from ..utils.logger import logger


class Strategy(ABC):
    """Abstract base class for trading strategies."""

    def __init__(
        self,
        name: str,
        symbols: list[str],
        market_data: MarketDataManager,
        order_manager: OrderManager,
    ):
        """
        Initialize Strategy.

        Args:
            name: Strategy name
            symbols: List of symbols to trade
            market_data: Market data manager instance
            order_manager: Order manager instance
        """
        self.name = name
        self.symbols = symbols
        self.market_data = market_data
        self.order_manager = order_manager
        self._running = False

        logger.info(f"Strategy initialized: {name} - Symbols: {symbols}")

    async def start(self) -> None:
        """Start the strategy."""
        if self._running:
            logger.warning(f"Strategy {self.name} already running")
            return

        self._running = True
        logger.info(f"Strategy {self.name} started")

        # Register callbacks
        self.market_data.add_orderbook_callback(self._on_orderbook_update)
        self.market_data.add_trade_callback(self._on_trade_update)

        # Call user initialization hook
        await self.on_start()

    async def stop(self) -> None:
        """Stop the strategy."""
        if not self._running:
            return

        self._running = False
        logger.info(f"Strategy {self.name} stopped")

        # Call user cleanup hook
        await self.on_stop()

    @abstractmethod
    async def on_start(self) -> None:
        """
        Called when strategy starts.
        Override this to implement initialization logic.
        """
        pass

    @abstractmethod
    async def on_stop(self) -> None:
        """
        Called when strategy stops.
        Override this to implement cleanup logic.
        """
        pass

    @abstractmethod
    async def on_orderbook_update(self, symbol: str, orderbook: OrderBook) -> None:
        """
        Called when orderbook is updated.

        Args:
            symbol: Trading symbol
            orderbook: Updated orderbook with integer prices
        """
        pass

    @abstractmethod
    async def on_trades_update(self, symbol: str, trades: list[Trade]) -> None:
        """
        Called when new trades are received.

        Args:
            symbol: Trading symbol
            trades: List of new trades with integer prices
        """
        pass

    @abstractmethod
    async def on_tick(self) -> None:
        """
        Called periodically (e.g., every second).
        Override this for time-based logic.
        """
        pass

    async def _on_orderbook_update(self, symbol: str, orderbook: OrderBook) -> None:
        """Internal orderbook update handler."""
        if symbol in self.symbols:
            try:
                await self.on_orderbook_update(symbol, orderbook)
            except Exception as e:
                logger.error(
                    f"Error in {self.name}.on_orderbook_update: {e}", exc_info=True
                )

    async def _on_trade_update(self, symbol: str, trades: list[Trade]) -> None:
        """Internal trade update handler."""
        if symbol in self.symbols:
            try:
                await self.on_trades_update(symbol, trades)
            except Exception as e:
                logger.error(
                    f"Error in {self.name}.on_trades_update: {e}", exc_info=True
                )

    # Helper methods for placing orders

    async def buy_limit(
        self, symbol: str, size: int, price: int, client_order_id: str | None = None
    ) -> Order:
        """
        Place a buy limit order.

        Args:
            symbol: Trading symbol
            size: Order size (contracts) as integer
            price: Limit price as integer
            client_order_id: Optional client order ID

        Returns:
            Placed order
        """
        order = Order(
            symbol=symbol,
            side="buy",
            order_type="limit_order",
            size=size,
            price=price,
            client_order_id=client_order_id,
        )
        return await self.order_manager.place_order(order)

    async def sell_limit(
        self, symbol: str, size: int, price: int, client_order_id: str | None = None
    ) -> Order:
        """
        Place a sell limit order.

        Args:
            symbol: Trading symbol
            size: Order size (contracts) as integer
            price: Limit price as integer
            client_order_id: Optional client order ID

        Returns:
            Placed order
        """
        order = Order(
            symbol=symbol,
            side="sell",
            order_type="limit_order",
            size=size,
            price=price,
            client_order_id=client_order_id,
        )
        return await self.order_manager.place_order(order)

    async def buy_market(
        self, symbol: str, size: int, client_order_id: str | None = None
    ) -> Order:
        """
        Place a buy market order.

        Args:
            symbol: Trading symbol
            size: Order size (contracts) as integer
            client_order_id: Optional client order ID

        Returns:
            Placed order
        """
        order = Order(
            symbol=symbol,
            side="buy",
            order_type="market_order",
            size=size,
            client_order_id=client_order_id,
        )
        return await self.order_manager.place_order(order)

    async def sell_market(
        self, symbol: str, size: int, client_order_id: str | None = None
    ) -> Order:
        """
        Place a sell market order.

        Args:
            symbol: Trading symbol
            size: Order size (contracts) as integer
            client_order_id: Optional client order ID

        Returns:
            Placed order
        """
        order = Order(
            symbol=symbol,
            side="sell",
            order_type="market_order",
            size=size,
            client_order_id=client_order_id,
        )
        return await self.order_manager.place_order(order)

    async def cancel_order(self, client_order_id: str) -> bool:
        """
        Cancel an order.

        Args:
            order_id: Client Order ID to cancel

        Returns:
            True if successful
        """
        return await self.order_manager.cancel_order(client_order_id)

    async def cancel_all_orders(self, symbol: str | None = None) -> int:
        """
        Cancel all orders.

        Args:
            symbol: Optional symbol to filter by

        Returns:
            Number of orders cancelled
        """
        return await self.order_manager.cancel_all_orders(symbol)

    def get_best_bid(self, symbol: str) -> int | None:
        """
        Get best bid price as integer.

        Args:
            symbol: Trading symbol

        Returns:
            Best bid price or None
        """
        return self.market_data.get_best_bid(symbol)

    def get_best_ask(self, symbol: str) -> int | None:
        """
        Get best ask price as integer.

        Args:
            symbol: Trading symbol

        Returns:
            Best ask price or None
        """
        return self.market_data.get_best_ask(symbol)

    def get_mid_price(self, symbol: str) -> int | None:
        """
        Get mid price as integer.

        Args:
            symbol: Trading symbol

        Returns:
            Mid price or None
        """
        return self.market_data.get_mid_price(symbol)

    def get_spread(self, symbol: str) -> int | None:
        """
        Get spread as integer.

        Args:
            symbol: Trading symbol

        Returns:
            Spread or None
        """
        return self.market_data.get_spread(symbol)

    def get_orderbook(self, symbol: str) -> OrderBook | None:
        """
        Get current orderbook.

        Args:
            symbol: Trading symbol

        Returns:
            OrderBook or None
        """
        return self.market_data.get_orderbook(symbol)

    def get_recent_trades(self, symbol: str, limit: int | None = None) -> list[Trade]:
        """
        Get recent trades.

        Args:
            symbol: Trading symbol
            limit: Maximum number of trades

        Returns:
            List of trades
        """
        return self.market_data.get_trades(symbol, limit)

    @property
    def is_running(self) -> bool:
        """Check if strategy is running."""
        return self._running
