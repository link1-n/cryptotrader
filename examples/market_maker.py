"""Market maker strategy example for Delta Exchange."""

import asyncio

from deltatrader import Config, Strategy, TradingEngine
from deltatrader.models.orderbook import OrderBook
from deltatrader.models.trade import Trade
from deltatrader.utils.logger import logger


class MarketMakerStrategy(Strategy):
    """
    Market making strategy that quotes bid/ask around mid price.

    Features:
    - Uses integer arithmetic for all calculations
    - Position-aware quoting (reduces size near limits)
    - Automatic order cancellation and replacement
    - Multi-symbol support
    """

    def __init__(
        self,
        name: str,
        symbols: list,
        market_data,
        order_manager,
        spread_ticks: int = 5,
        order_size: int = 1,
        max_position: int = 10,
        quote_refresh_interval: int = 5,  # seconds
    ):
        """
        Initialize market maker.

        Args:
            name: Strategy name
            symbols: List of symbols to quote
            market_data: Market data manager
            order_manager: Order manager
            spread_ticks: Number of ticks away from mid to quote
            order_size: Size per order (contracts)
            max_position: Maximum position per symbol
            quote_refresh_interval: How often to refresh quotes (seconds)
        """
        super().__init__(name, symbols, market_data, order_manager)

        # Strategy parameters
        self.spread_ticks = spread_ticks
        self.order_size = order_size
        self.max_position = max_position
        self.quote_refresh_interval = quote_refresh_interval

        # State tracking
        self.positions: dict[str, int] = {symbol: 0 for symbol in symbols}
        self.active_orders: dict[str, list] = {symbol: [] for symbol in symbols}
        self.last_quote_time: dict[str, float] = {symbol: 0.0 for symbol in symbols}

        # Statistics
        self.total_updates = 0
        self.total_trades_seen = 0

    async def on_start(self) -> None:
        """Called when strategy starts."""
        logger.info("=" * 70)
        logger.info(f"{self.name}: Market Maker Strategy Started")
        logger.info("=" * 70)
        logger.info(f"Symbols: {self.symbols}")
        logger.info(f"Spread: {self.spread_ticks} ticks")
        logger.info(f"Order size: {self.order_size} contracts")
        logger.info(f"Max position: ±{self.max_position} contracts")
        logger.info(f"Quote refresh: every {self.quote_refresh_interval}s")
        logger.info("=" * 70)

    async def on_stop(self) -> None:
        """Called when strategy stops."""
        logger.info("=" * 70)
        logger.info(f"{self.name}: Stopping - Final Statistics")
        logger.info("=" * 70)
        logger.info(f"Total orderbook updates: {self.total_updates}")
        logger.info(f"Total trades seen: {self.total_trades_seen}")
        logger.info(f"Final positions: {self.positions}")
        logger.info("=" * 70)

        # Cancel all orders
        logger.info("Cancelling all orders...")
        for symbol in self.symbols:
            await self.cancel_all_orders(symbol)

    async def on_orderbook_update(self, symbol: str, orderbook: OrderBook) -> None:
        """
        Called on orderbook updates.

        Strategy logic:
        1. Check if we need to refresh quotes
        2. Calculate optimal bid/ask levels
        3. Cancel old orders and place new ones
        """
        self.total_updates += 1

        # Get current time
        current_time = asyncio.get_event_loop().time()

        # Check if we should update quotes
        time_since_last_quote = current_time - self.last_quote_time.get(symbol, 0)
        should_refresh = time_since_last_quote >= self.quote_refresh_interval

        if not should_refresh:
            return

        try:
            # Get mid price
            mid_price = orderbook.get_mid_price()
            if mid_price == 0:
                return

            best_bid, _ = orderbook.get_best_bid()
            best_ask, _ = orderbook.get_best_ask()
            spread = orderbook.get_spread()

            # Log market state periodically
            if self.total_updates % 50 == 0:
                logger.info(
                    f"{self.name} {symbol}: "
                    f"mid={mid_price}, spread={spread}, "
                    f"position={self.positions[symbol]}"
                )

            # Update quotes
            await self._update_quotes(symbol, mid_price, spread, orderbook)

            # Update last quote time
            self.last_quote_time[symbol] = current_time

        except Exception as e:
            logger.error(
                f"{self.name} {symbol}: Error in orderbook update: {e}",
                exc_info=True,
            )

    async def on_trades_update(self, symbol: str, trades: list[Trade]) -> None:
        """Called when new trades arrive."""
        self.total_trades_seen += len(trades)

        # Log trade flow
        buy_volume = sum(t.size for t in trades if t.side == "buy")
        sell_volume = sum(t.size for t in trades if t.side == "sell")

        if buy_volume > 0 or sell_volume > 0:
            logger.debug(
                f"{self.name} {symbol}: "
                f"Trade flow - buys={buy_volume}, sells={sell_volume}"
            )

        # Could use this to adjust quotes based on trade flow imbalance
        # Example: if buy_volume > sell_volume * 2, widen bid or narrow ask

    async def on_tick(self) -> None:
        """Called every second for periodic checks."""
        # Check position limits
        for symbol in self.symbols:
            position = self.positions[symbol]
            if abs(position) > self.max_position:
                logger.warning(
                    f"{self.name} {symbol}: Position limit exceeded! "
                    f"Position={position}, Limit={self.max_position}"
                )

                # In production, you would reduce position here
                # For now, just cancel quotes
                await self.cancel_all_orders(symbol)

    async def _update_quotes(
        self, symbol: str, mid_price: int, spread: int, orderbook: OrderBook
    ) -> None:
        """
        Update bid/ask quotes for a symbol.

        Args:
            symbol: Trading symbol
            mid_price: Current mid price (integer)
            spread: Current spread (integer)
            orderbook: Current orderbook
        """
        position = self.positions[symbol]

        # Calculate quote levels
        # Place bid below mid, ask above mid
        half_spread = max(spread // 2, 1)

        bid_price = mid_price - half_spread - self.spread_ticks
        ask_price = mid_price + half_spread + self.spread_ticks

        # Adjust sizes based on position
        bid_size = self.order_size
        ask_size = self.order_size

        # Don't quote if at position limits
        quote_bid = position < self.max_position
        quote_ask = position > -self.max_position

        # Reduce size if approaching limits
        if position > self.max_position // 2:
            bid_size = max(1, bid_size // 2)
        if position < -self.max_position // 2:
            ask_size = max(1, ask_size // 2)

        # Cancel existing orders for this symbol
        if self.active_orders[symbol]:
            for order_id in self.active_orders[symbol]:
                try:
                    await self.cancel_order(order_id)
                except Exception as e:
                    logger.debug(f"Failed to cancel {order_id}: {e}")
            self.active_orders[symbol].clear()

        # Place new bid
        if quote_bid:
            try:
                bid_order = await self.buy_limit(symbol, bid_size, bid_price)
                if bid_order.client_order_id:
                    self.active_orders[symbol].append(bid_order.client_order_id)
                    logger.info(
                        f"{self.name} {symbol}: "
                        f"BID {bid_size} @ {bid_price} (ID: {bid_order.client_order_id})"
                    )
            except Exception as e:
                logger.error(f"{self.name} {symbol}: Failed to place bid: {e}")

        # Place new ask
        if quote_ask:
            try:
                ask_order = await self.sell_limit(symbol, ask_size, ask_price)
                if ask_order.client_order_id:
                    self.active_orders[symbol].append(ask_order.client_order_id)
                    logger.info(
                        f"{self.name} {symbol}: "
                        f"ASK {ask_size} @ {ask_price} (ID: {ask_order.client_order_id})"
                    )
            except Exception as e:
                logger.error(f"{self.name} {symbol}: Failed to place ask: {e}")


async def main():
    """Main function."""
    logger.info("=" * 70)
    logger.info("Delta Exchange Market Maker Strategy")
    logger.info("=" * 70)

    # Check configuration
    if not Config.API_KEY or not Config.API_SECRET:
        logger.error(
            "API credentials not found!\n"
            "Please set DELTA_API_KEY and DELTA_API_SECRET in .env file"
        )
        return

    logger.info(f"Environment: {Config.ENVIRONMENT}")

    # Symbols to trade
    symbols = ["BTCUSD"]  # Start with one symbol
    logger.info(f"Trading symbols: {symbols}")

    # Create trading engine
    engine = TradingEngine()

    try:
        # Initialize engine
        logger.info("Initializing trading engine...")
        await engine.initialize(symbols=symbols)

        # Create market maker strategy
        # For live trading, use conservative parameters
        # More conservative for live trading
        spread_ticks = 10  # Wider spread for safety
        order_size = 1  # Keep size small
        max_position = 5  # Lower position limit
        refresh_interval = 10  # Refresh less frequently

        logger.info("Strategy parameters:")
        logger.info(f"Spread: {spread_ticks} ticks")
        logger.info(f"Order size: {order_size} contracts")
        logger.info(f"Max position: ±{max_position} contracts")
        logger.info(f"Refresh interval: {refresh_interval}s")

        strategy = MarketMakerStrategy(
            name="MM",
            symbols=symbols,
            market_data=engine.market_data,
            order_manager=engine.order_manager,
            spread_ticks=spread_ticks,
            order_size=order_size,
            max_position=max_position,
            quote_refresh_interval=refresh_interval,
        )

        # Add strategy to engine
        await engine.add_strategy(strategy)

        # Start engine
        logger.info("Starting trading engine...")
        await engine.start()

        # Run
        logger.info("Market maker running. Press Ctrl+C to stop.")

        # Keep running and print status
        while engine.is_running:
            await asyncio.sleep(30)

            # Print status every 30 seconds
            for symbol in symbols:
                summary = engine.get_market_data_summary(symbol)
                logger.info(f"Status: {summary}")

                # Get open orders
                open_orders = await engine.order_manager.get_open_orders(symbol)
                logger.info(f"Open orders for {symbol}: {len(open_orders)}")

                # Show position if live trading
                # try:
                #     positions = await engine.get_positions()
                #     for pos in positions:
                #         if pos.get("product", {}).get("symbol") == symbol:
                #             logger.info(f"Position for {symbol}: {pos}")
                # except Exception as e:
                #     logger.debug(f"Could not fetch positions: {e}")

    except KeyboardInterrupt:
        logger.info("\nKeyboard interrupt received. Shutting down...")
    except Exception as e:
        logger.error(f"Error in market maker: {e}", exc_info=True)
    finally:
        # Clean shutdown
        await engine.stop()
        logger.info("Market maker stopped.")


if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())
