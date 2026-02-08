import asyncio
import uuid

from ..models.order import Order
from ..utils.integer_conversion import IntegerConverter
from ..utils.logger import logger
from ..utils.timing import get_timestamp_us
from .order_manager import OrderManager


class PaperOrderManager(OrderManager):
    """Paper trading order manager (simulated orders)."""

    def __init__(self, converter: IntegerConverter):
        """
        Initialize PaperOrderManager.

        Args:
            converter: Integer converter instance
        """
        super().__init__(converter)
        self._order_counter = 0
        self._simulated_latency = 0.05  # 50ms simulated latency

    async def place_order(self, order: Order) -> Order:
        """
        Place a simulated order.

        Args:
            order: Order to place

        Returns:
            Updated order with ID and status
        """
        # Simulate network latency
        await asyncio.sleep(self._simulated_latency)

        # Generate order ID
        self._order_counter += 1
        order.exchange_order_id = self._order_counter
        if not order.client_order_id:
            # Use UUID hex (32 chars) for consistency with live orders
            order.client_order_id = uuid.uuid4().hex

        # Set status to open (paper orders are instantly accepted)
        order.status = "open"
        order.timestamp = get_timestamp_us()

        # Store order
        self._orders[order.client_order_id] = order

        logger.info(
            f"[PAPER] Order placed: {order.symbol} {order.side} {order.size} @ {order.price} - ClientOrderID: {order.client_order_id}"
        )

        # Simulate immediate fill for market orders
        if order.order_type == "market_order":
            asyncio.create_task(self._simulate_fill(order))

        return order

    async def cancel_order(self, client_order_id: str) -> bool:
        """
        Cancel a simulated order.

        Args:
            order_id: Order ID to cancel

        Returns:
            True if successful
        """
        # Simulate network latency
        await asyncio.sleep(self._simulated_latency)

        if client_order_id in self._orders:
            order = self._orders[client_order_id]
            if order.status in ["open", "pending"]:
                order.status = "cancelled"
                logger.info(f"[PAPER] Order cancelled: {client_order_id}")
                return True

        logger.warning(f"[PAPER] Order not found or already closed: {client_order_id}")
        return False

    async def cancel_all_orders(self, symbol: str | None = None) -> int:
        """
        Cancel all simulated orders.

        Args:
            symbol: Optional symbol to filter by

        Returns:
            Number of orders cancelled
        """
        # Simulate network latency
        await asyncio.sleep(self._simulated_latency)

        count = 0
        for order in self._orders.values():
            if symbol is None or order.symbol == symbol:
                if order.status in ["open", "pending"]:
                    order.status = "cancelled"
                    count += 1

        logger.info(f"[PAPER] Cancelled {count} orders for {symbol or 'all symbols'}")
        return count

    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        """
        Get open simulated orders.

        Args:
            symbol: Optional symbol to filter by

        Returns:
            List of open orders
        """
        orders = []
        for order in self._orders.values():
            if order.status in ["open", "pending"]:
                if symbol is None or order.symbol == symbol:
                    orders.append(order)
        return orders

    async def _simulate_fill(self, order: Order, delay: float = 0.1) -> None:
        """
        Simulate order fill after a delay.

        Args:
            order: Order to fill
            delay: Delay before fill in seconds
        """
        await asyncio.sleep(delay)

        if order.status == "open":
            order.status = "filled"
            order.filled_size = order.size
            order.average_fill_price = order.price

            logger.info(
                f"[PAPER] Order filled: {order.client_order_id} - {order.symbol} {order.side} {order.size} @ {order.price}"
            )

    def simulate_fill(self, order_id: str, fill_price: int | None = None) -> bool:
        """
        Manually simulate a fill for testing.

        Args:
            order_id: Order ID to fill
            fill_price: Fill price (uses order price if None)

        Returns:
            True if successful
        """
        if order_id in self._orders:
            order = self._orders[order_id]
            if order.status == "open":
                order.status = "filled"
                order.filled_size = order.size
                order.average_fill_price = fill_price or order.price

                logger.info(
                    f"[PAPER] Manual fill: {order_id} - {order.symbol} {order.side} {order.size} @ {order.average_fill_price}"
                )
                return True

        return False
