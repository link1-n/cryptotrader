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
        self._reconciliation_task: asyncio.Task | None = None
        self._reconciliation_interval = 30  # seconds
        self._running = False

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

    async def edit_order(
        self,
        client_order_id: str,
        new_size: int | None = None,
        new_price: int | None = None,
    ) -> Order | None:
        """
        Edit an existing order in-place using Delta Exchange's native edit_order API.

        This modifies the order without cancelling it, preserving queue position.
        Delta Exchange supports editing size and limit_price for open orders.

        Args:
            client_order_id: Order ID to edit
            new_size: New size (contracts), None to keep existing
            new_price: New price (integer), None to keep existing

        Returns:
            Updated order if successful, None otherwise
        """
        # Simulate network latency
        await asyncio.sleep(self._simulated_latency)

        # Get the existing order
        order = self.get_order(client_order_id)
        if not order:
            logger.error(f"[PAPER] Order not found for edit: {client_order_id}")
            return None

        # Check if order is still open
        if order.status not in ["open", "pending"]:
            logger.error(
                f"[PAPER] Cannot edit order {client_order_id}: status is {order.status}"
            )
            return None

        # Use existing values if not specified
        size = new_size if new_size is not None else order.size
        price = new_price if new_price is not None else order.price

        # Check if anything actually changed
        if size == order.size and price == order.price:
            logger.info(
                f"[PAPER] No changes for order {client_order_id}, skipping edit"
            )
            return order

        logger.info(
            f"[PAPER] Editing order {client_order_id}: "
            f"size {order.size}->{size}, price {order.price}->{price}"
        )

        # Update order in place (paper trading advantage - instant edit)
        order.size = size
        order.price = price
        order.timestamp = get_timestamp_us()

        logger.info(f"[PAPER] Order edited successfully: {client_order_id}")
        return order

    async def start_reconciliation(self) -> None:
        """Start periodic order reconciliation."""
        if self._running:
            logger.warning("[PAPER] Order reconciliation already running")
            return

        self._running = True
        self._reconciliation_task = asyncio.create_task(self._reconciliation_loop())
        logger.info(
            f"[PAPER] Started order reconciliation (interval: {self._reconciliation_interval}s)"
        )

    async def stop_reconciliation(self) -> None:
        """Stop periodic order reconciliation."""
        if not self._running:
            return

        logger.info("[PAPER] Stopping order reconciliation...")
        self._running = False

        if self._reconciliation_task:
            self._reconciliation_task.cancel()
            try:
                await self._reconciliation_task
            except asyncio.CancelledError:
                pass

        logger.info("[PAPER] Order reconciliation stopped")

    async def _reconciliation_loop(self) -> None:
        """Periodic reconciliation loop."""
        try:
            while self._running:
                await asyncio.sleep(self._reconciliation_interval)

                try:
                    stats = await self.reconcile_orders()
                    logger.debug(f"[PAPER] Reconciliation stats: {stats}")
                except Exception as e:
                    logger.error(
                        f"[PAPER] Error in reconciliation loop: {e}", exc_info=True
                    )

        except asyncio.CancelledError:
            logger.debug("[PAPER] Reconciliation loop cancelled")

    def set_reconciliation_interval(self, interval: int) -> None:
        """
        Set the reconciliation interval.

        Args:
            interval: Interval in seconds (minimum 5)
        """
        if interval < 5:
            logger.warning("[PAPER] Reconciliation interval must be at least 5 seconds")
            interval = 5

        self._reconciliation_interval = interval
        logger.info(f"[PAPER] Reconciliation interval set to {interval}s")
