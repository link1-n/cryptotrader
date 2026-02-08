"""Trading engine that orchestrates all components."""

import asyncio

from ..client.rest import RestClient
from ..client.websocket import WebSocketClient
from ..models.product import Product
from ..strategies.base import Strategy
from ..utils.config import Config
from ..utils.integer_conversion import IntegerConverter
from ..utils.logger import logger
from .live_order_manager import LiveOrderManager
from .market_data import MarketDataManager
from .order_manager import OrderManager
from .paper_order_manager import PaperOrderManager


class TradingEngine:
    """Main trading engine that manages all components."""

    def __init__(self):
        """
        Initialize TradingEngine.
        """

        # Validate configuration
        if not Config.validate():
            raise ValueError(
                "Invalid configuration - API_KEY and API_SECRET required. "
                "Please set DELTA_API_KEY and DELTA_API_SECRET in .env file"
            )

        # Initialize components
        self.converter = IntegerConverter()
        self.ws_client = WebSocketClient()
        self.rest_client = RestClient()
        self.market_data = MarketDataManager(self.ws_client, self.converter)

        # Initialize order manager (live or paper)
        if Config.ORDER_DESTINATION == "paper":
            self.order_manager: OrderManager = PaperOrderManager(self.converter)
            logger.info("Using PaperOrderManager for demo mode")
        elif Config.ORDER_DESTINATION == "exchange":
            self.order_manager = LiveOrderManager(self.rest_client, self.converter)
            logger.info("Using LiveOrderManager for live trading")
        else:
            raise ValueError("Invalid Configuration - set order destination")

        # Strategies
        self.strategies: list[Strategy] = []

        # Tasks
        self._tick_task: asyncio.Task | None = None
        self._running = False

        # Products
        self.products: list[Product] = []

    async def initialize(self, symbols: list[str] | None = None) -> None:
        """
        Initialize the engine.

        Args:
            symbols: Optional list of symbols to fetch products for
        """
        logger.info("Initializing trading engine...")

        # Connect REST client
        await self.rest_client.connect()

        # Fetch products
        if symbols:
            logger.info(f"Fetching products for symbols: {symbols}")
            for symbol in symbols:
                try:
                    product = await self.rest_client.get_product(symbol)
                    if product:
                        self.products.append(product)
                        self.converter.register_product(product)
                        self.order_manager.register_product(product)
                        logger.info(f"Registered product: {symbol}")
                    else:
                        logger.warning(f"Product not found: {symbol}")
                except Exception as e:
                    logger.error(f"Failed to fetch product {symbol}: {e}")
        else:
            # Fetch all perpetual futures
            logger.info("Fetching all perpetual futures products...")
            self.products = await self.rest_client.get_products(
                contract_types=["perpetual_futures"]
            )

            # Register products
            for product in self.products:
                self.converter.register_product(product)
                self.order_manager.register_product(product)

            logger.info(f"Registered {len(self.products)} products")

        # Connect WebSocket
        await self.ws_client.connect(authenticate=True)
        await self.ws_client.wait_connected(timeout=10.0)

        if not self.ws_client.is_connected:
            raise ConnectionError("Failed to connect to WebSocket")

        logger.info("Trading engine initialized successfully")

    async def add_strategy(self, strategy: Strategy) -> None:
        """
        Add a strategy to the engine.

        Args:
            strategy: Strategy instance to add
        """
        self.strategies.append(strategy)
        logger.info(f"Added strategy: {strategy.name}")

        # Subscribe to market data for strategy symbols
        for symbol in strategy.symbols:
            await self.market_data.subscribe_orderbook(symbol)
            await self.market_data.subscribe_trades(symbol)
            logger.info(f"Subscribed to market data for {symbol}")

    async def start(self) -> None:
        """Start the trading engine and all strategies."""
        if self._running:
            logger.warning("Trading engine already running")
            return

        self._running = True
        logger.info("Starting trading engine...")

        # Start all strategies
        for strategy in self.strategies:
            await strategy.start()

        # Start tick loop
        self._tick_task = asyncio.create_task(self._tick_loop())

        logger.info("Trading engine started")

    async def stop(self) -> None:
        """Stop the trading engine and all strategies."""
        if not self._running:
            return

        logger.info("Stopping trading engine...")
        self._running = False

        # Stop tick loop
        if self._tick_task:
            self._tick_task.cancel()
            try:
                await self._tick_task
            except asyncio.CancelledError:
                pass

        # Stop all strategies
        for strategy in self.strategies:
            await strategy.stop()

        # Cancel all orders
        logger.info("Cancelling all open orders...")
        await self.order_manager.cancel_all_orders()

        # Disconnect clients
        await self.ws_client.disconnect()
        await self.rest_client.close()

        # Cleanup market data
        await self.market_data.cleanup()

        logger.info("Trading engine stopped")

    async def _tick_loop(self) -> None:
        """Periodic tick loop for time-based strategy updates."""
        try:
            while self._running:
                await asyncio.sleep(1.0)  # 1 second tick

                # Call on_tick for all strategies
                for strategy in self.strategies:
                    if strategy.is_running:
                        try:
                            await strategy.on_tick()
                        except Exception as e:
                            logger.error(
                                f"Error in {strategy.name}.on_tick: {e}", exc_info=True
                            )

        except asyncio.CancelledError:
            logger.debug("Tick loop cancelled")

    async def run(self) -> None:
        """
        Run the trading engine until interrupted.

        This is a convenience method that starts the engine and waits indefinitely.
        """
        try:
            await self.start()
            logger.info("Trading engine running. Press Ctrl+C to stop.")

            # Keep running until interrupted
            while self._running:
                await asyncio.sleep(1.0)

        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")
        except Exception as e:
            logger.error(f"Error in trading engine: {e}", exc_info=True)
        finally:
            await self.stop()

    def get_product(self, symbol: str) -> Product | None:
        """
        Get product by symbol.

        Args:
            symbol: Trading symbol

        Returns:
            Product or None if not found
        """
        for product in self.products:
            if product.symbol == symbol:
                return product
        return None

    async def get_positions(self) -> list[dict]:
        """
        Get all current positions.

        Returns:
            List of position data
        """
        if isinstance(self.order_manager, LiveOrderManager):
            return await self.rest_client.get_positions()
        else:
            logger.warning("Positions not available in paper trading mode")
            return []

    async def get_wallet_balance(self) -> dict:
        """
        Get wallet balance.

        Returns:
            Wallet balance data
        """
        if isinstance(self.order_manager, LiveOrderManager):
            return await self.rest_client.get_wallet_balance()
        else:
            logger.warning("Wallet balance not available in paper trading mode")
            return {}

    @property
    def is_running(self) -> bool:
        """Check if trading engine is running."""
        return self._running

    def get_market_data_summary(self, symbol: str) -> dict:
        """
        Get market data summary for a symbol.

        Args:
            symbol: Trading symbol

        Returns:
            Dictionary with market data summary
        """
        orderbook = self.market_data.get_orderbook(symbol)
        recent_trades = self.market_data.get_trades(symbol, limit=10)

        summary = {
            "symbol": symbol,
            "orderbook_available": orderbook is not None,
            "best_bid": None,
            "best_ask": None,
            "mid_price": None,
            "spread": None,
            "recent_trades_count": len(recent_trades),
        }

        if orderbook:
            best_bid = orderbook.get_best_bid()
            best_ask = orderbook.get_best_ask()

            summary.update(
                {
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                    "mid_price": orderbook.get_mid_price(),
                    "spread": orderbook.get_spread(),
                    "sequence_no": orderbook.sequence_no,
                }
            )

        return summary

    async def __aenter__(self):
        """Context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        await self.stop()
