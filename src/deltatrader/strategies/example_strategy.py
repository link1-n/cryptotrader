"""Example strategy demonstrating basic market making."""

from ..models.orderbook import OrderBook
from ..models.trade import Trade
from ..utils.logger import logger
from .base import Strategy


class ExampleMarketMaker(Strategy):
    """
    Simple market making strategy example.

    This strategy:
    - Quotes bid/ask around mid price
    - Uses integer arithmetic for all calculations
    - Demonstrates multi-symbol trading
    """

    def __init__(self, name: str, symbols: list[str], market_data, order_manager):
        """
        Initialize example market maker.

        Args:
            name: Strategy name
            symbols: List of symbols to trade
            market_data: Market data manager
            order_manager: Order manager
        """
        super().__init__(name, symbols, market_data, order_manager)

        # Strategy parameters (all as integers)
        self.spread_offset = 2  # Ticks away from mid price
        self.order_size = 1  # Contracts per order
        self.max_position = 10  # Max position per symbol

        # Track positions (symbol -> net position)
        self.positions = {symbol: 0 for symbol in symbols}

        # Track active orders (symbol -> list of order_ids)
        self.active_orders = {symbol: [] for symbol in symbols}

    async def on_start(self) -> None:
        """Called when strategy starts."""
        logger.info(f"{self.name}: Starting with symbols {self.symbols}")
        logger.info(
            f"{self.name}: Parameters - spread_offset={self.spread_offset}, size={self.order_size}"
        )

    async def on_stop(self) -> None:
        """Called when strategy stops."""
        logger.info(f"{self.name}: Stopping - cancelling all orders")

        # Cancel all orders on stop
        for symbol in self.symbols:
            await self.cancel_all_orders(symbol)

    async def on_orderbook_update(self, symbol: str, orderbook: OrderBook) -> None:
        """
        Called when orderbook updates.

        Strategy logic:
        1. Get mid price
        2. Calculate quote levels
        3. Place/update orders
        """
        if not self.is_running:
            return

        try:
            # Get mid price (integer)
            mid_price = orderbook.get_mid_price()
            if mid_price == 0:
                return

            best_bid_price, best_bid_size = orderbook.get_best_bid()
            best_ask_price, best_ask_size = orderbook.get_best_ask()

            # Log market data (every N updates to avoid spam)
            if orderbook.sequence_no % 100 == 0:
                logger.debug(
                    f"{self.name} {symbol}: mid={mid_price}, "
                    f"bid={best_bid_price}@{best_bid_size}, "
                    f"ask={best_ask_price}@{best_ask_size}, "
                    f"spread={orderbook.get_spread()}"
                )

            # Simple market making logic
            await self._update_quotes(symbol, mid_price, orderbook)

        except Exception as e:
            logger.error(f"{self.name}: Error in orderbook update: {e}", exc_info=True)

    async def on_trades_update(self, symbol: str, trades: list[Trade]) -> None:
        """
        Called when new trades arrive.

        Args:
            symbol: Trading symbol
            trades: List of new trades (all with integer prices)
        """
        if not self.is_running:
            return

        # Log trade flow
        for trade in trades:
            logger.debug(
                f"{self.name} {symbol}: Trade - {trade.side} "
                f"{trade.size} @ {trade.price}"
            )

        # You could track trade imbalance, aggressive flow, etc.
        # Example: count buy vs sell volume
        buy_volume = sum(t.size for t in trades if t.side == "buy")
        sell_volume = sum(t.size for t in trades if t.side == "sell")

        if len(trades) > 0:
            logger.debug(
                f"{self.name} {symbol}: Trade flow - "
                f"buy_vol={buy_volume}, sell_vol={sell_volume}"
            )

    async def on_tick(self) -> None:
        """
        Called periodically (e.g., every second).

        Use this for time-based checks and housekeeping.
        """
        if not self.is_running:
            return

        # Example: Check positions and risk limits
        for symbol in self.symbols:
            position = self.positions[symbol]
            if abs(position) > self.max_position:
                logger.warning(
                    f"{self.name} {symbol}: Position limit exceeded - "
                    f"position={position}, limit={self.max_position}"
                )
                # Could cancel quotes or reduce position here

    async def _update_quotes(
        self, symbol: str, mid_price: int, orderbook: OrderBook
    ) -> None:
        """
        Update bid/ask quotes for a symbol.

        Args:
            symbol: Trading symbol
            mid_price: Current mid price (integer)
            orderbook: Current orderbook
        """
        # Check position limits
        position = self.positions[symbol]

        # Calculate quote levels (integer arithmetic)
        # Place bid slightly below mid, ask slightly above mid
        spread = orderbook.get_spread()

        # Use tick-based offset
        bid_price = mid_price - (spread // 2) - self.spread_offset
        ask_price = mid_price + (spread // 2) + self.spread_offset

        # Skip if position limits exceeded
        if abs(position) >= self.max_position:
            logger.debug(
                f"{self.name} {symbol}: Skipping quotes - position limit reached"
            )
            return

        # Reduce quote size if near position limit
        bid_size = self.order_size
        ask_size = self.order_size

        if position > self.max_position // 2:
            # Long position, reduce bid size
            bid_size = max(1, bid_size // 2)
        elif position < -self.max_position // 2:
            # Short position, reduce ask size
            ask_size = max(1, ask_size // 2)

        # For this simple example, we'll place new orders each time
        # In production, you'd want to check existing orders and only update if needed

        # Cancel old orders first (simplified approach)
        if self.active_orders[symbol]:
            for order_id in self.active_orders[symbol]:
                await self.cancel_order(order_id)
            self.active_orders[symbol].clear()

        # Place new bid
        if position < self.max_position:
            try:
                bid_order = await self.buy_limit(symbol, bid_size, bid_price)
                if bid_order.client_order_id:
                    self.active_orders[symbol].append(bid_order.client_order_id)
                    logger.debug(
                        f"{self.name} {symbol}: Placed bid - "
                        f"{bid_size} @ {bid_price} (ID: {bid_order.client_order_id})"
                    )
            except Exception as e:
                logger.error(f"{self.name} {symbol}: Failed to place bid: {e}")

        # Place new ask
        if position > -self.max_position:
            try:
                ask_order = await self.sell_limit(symbol, ask_size, ask_price)
                if ask_order.client_order_id:
                    self.active_orders[symbol].append(ask_order.client_order_id)
                    logger.debug(
                        f"{self.name} {symbol}: Placed ask - "
                        f"{ask_size} @ {ask_price} (ID: {ask_order.client_order_id})"
                    )
            except Exception as e:
                logger.error(f"{self.name} {symbol}: Failed to place ask: {e}")


class SimpleArbitrage(Strategy):
    """
    Example arbitrage strategy between two symbols.

    Demonstrates:
    - Multi-symbol monitoring
    - Integer price comparison
    - Conditional order placement
    """

    def __init__(
        self,
        name: str,
        symbol_a: str,
        symbol_b: str,
        market_data,
        order_manager,
        threshold: int = 100,  # Minimum price difference (integer)
    ):
        """
        Initialize arbitrage strategy.

        Args:
            name: Strategy name
            symbol_a: First symbol
            symbol_b: Second symbol
            market_data: Market data manager
            order_manager: Order manager
            threshold: Minimum price difference to trigger arb
        """
        super().__init__(name, [symbol_a, symbol_b], market_data, order_manager)
        self.symbol_a = symbol_a
        self.symbol_b = symbol_b
        self.threshold = threshold
        self.order_size = 1

    async def on_start(self) -> None:
        """Called when strategy starts."""
        logger.info(
            f"{self.name}: Monitoring {self.symbol_a} vs {self.symbol_b} "
            f"with threshold={self.threshold}"
        )

    async def on_stop(self) -> None:
        """Called when strategy stops."""
        await self.cancel_all_orders()

    async def on_orderbook_update(self, symbol: str, orderbook: OrderBook) -> None:
        """
        Called when orderbook updates.

        Check for arbitrage opportunities between two symbols.
        """
        if not self.is_running:
            return

        # Get both orderbooks
        ob_a = self.get_orderbook(self.symbol_a)
        ob_b = self.get_orderbook(self.symbol_b)

        if not ob_a or not ob_b:
            return

        # Get mid prices (integers)
        mid_a = ob_a.get_mid_price()
        mid_b = ob_b.get_mid_price()

        if mid_a == 0 or mid_b == 0:
            return

        # Calculate price difference (integer)
        diff = abs(mid_a - mid_b)

        # Check if arbitrage opportunity exists
        if diff > self.threshold:
            logger.info(
                f"{self.name}: Arbitrage opportunity detected - "
                f"{self.symbol_a} mid={mid_a}, {self.symbol_b} mid={mid_b}, "
                f"diff={diff}"
            )

            # Determine which side to trade
            if mid_a > mid_b:
                # Sell A, buy B
                logger.info(
                    f"{self.name}: Would sell {self.symbol_a}, buy {self.symbol_b}"
                )
            else:
                # Buy A, sell B
                logger.info(
                    f"{self.name}: Would buy {self.symbol_a}, sell {self.symbol_b}"
                )

            # In a real implementation, you would place orders here
            # For demo purposes, we just log

    async def on_trades_update(self, symbol: str, trades: list[Trade]) -> None:
        """Called when new trades arrive."""
        pass  # Not used in this strategy

    async def on_tick(self) -> None:
        """Called periodically."""
        pass  # Not used in this strategy
