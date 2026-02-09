"""Order management with live and paper trading implementations."""

from abc import ABC, abstractmethod

from ..models.order import Order
from ..models.product import Product
from ..utils.integer_conversion import IntegerConverter
from ..utils.logger import logger


class OrderManager(ABC):
    """Abstract base class for order management."""

    def __init__(self, converter: IntegerConverter):
        """
        Initialize OrderManager.

        Args:
            converter: Integer converter instance
        """
        self.converter = converter
        self._orders: dict[str, Order] = {}
        self._product_map: dict[str, int] = {}  # symbol -> product_id

    def register_product(self, product: Product) -> None:
        """
        Register a product for trading.

        Args:
            product: Product to register
        """
        self._product_map[product.symbol] = product.product_id
        logger.info(f"Registered product: {product.symbol} (ID: {product.product_id})")

    def get_product_id(self, symbol: str) -> int | None:
        """
        Get product ID for a symbol.

        Args:
            symbol: Trading symbol

        Returns:
            Product ID or None
        """
        return self._product_map.get(symbol)

    @abstractmethod
    async def place_order(self, order: Order) -> Order:
        """
        Place a new order.

        Args:
            order: Order to place

        Returns:
            Updated order with ID and status
        """
        pass

    @abstractmethod
    async def cancel_order(self, client_order_id: str) -> bool:
        """
        Cancel an order.

        Args:
            order_id: Order ID to cancel

        Returns:
            True if successful
        """
        pass

    @abstractmethod
    async def cancel_all_orders(self, symbol: str | None = None) -> int:
        """
        Cancel all orders.

        Args:
            symbol: Optional symbol to filter by

        Returns:
            Number of orders cancelled
        """
        pass

    @abstractmethod
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
        pass

    @abstractmethod
    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        """
        Get all open orders.

        Args:
            symbol: Optional symbol to filter by

        Returns:
            List of open orders
        """
        pass

    async def start_reconciliation(self) -> None:
        """
        Start periodic order reconciliation.

        Override this to implement reconciliation scheduling.
        """
        pass

    async def stop_reconciliation(self) -> None:
        """
        Stop periodic order reconciliation.

        Override this to stop reconciliation scheduling.
        """
        pass

    async def reconcile_orders(self) -> dict[str, int]:
        """
        Reconcile local orders with exchange state.

        Fetches all open orders from the exchange and updates local state.
        Marks orders as filled/cancelled if they no longer exist on exchange.

        Returns:
            Dictionary with reconciliation statistics
        """
        stats = {
            "synced": 0,
            "filled": 0,
            "cancelled": 0,
            "errors": 0,
        }

        try:
            # Get all open orders from exchange
            exchange_orders = await self.get_open_orders()
            exchange_order_ids = {
                order.client_order_id
                for order in exchange_orders
                if order.client_order_id
            }

            # Update local orders
            for client_order_id, local_order in list(self._orders.items()):
                # Skip already closed orders
                if local_order.status in ["filled", "cancelled", "rejected"]:
                    continue

                if client_order_id in exchange_order_ids:
                    # Order exists on exchange - update it
                    exchange_order = next(
                        o
                        for o in exchange_orders
                        if o.client_order_id == client_order_id
                    )
                    # Update local order with exchange state
                    local_order.status = exchange_order.status
                    local_order.filled_size = exchange_order.filled_size
                    local_order.average_fill_price = exchange_order.average_fill_price
                    stats["synced"] += 1
                else:
                    # Order doesn't exist on exchange - it was filled or cancelled
                    if local_order.filled_size >= local_order.size:
                        local_order.status = "filled"
                        stats["filled"] += 1
                    else:
                        local_order.status = "cancelled"
                        stats["cancelled"] += 1

                    logger.info(
                        f"Reconciled order {client_order_id}: "
                        f"{local_order.status} (filled {local_order.filled_size}/{local_order.size})"
                    )

        except Exception as e:
            logger.error(f"Error during order reconciliation: {e}", exc_info=True)
            stats["errors"] += 1

        logger.info(
            f"Order reconciliation complete: "
            f"synced={stats['synced']}, filled={stats['filled']}, "
            f"cancelled={stats['cancelled']}, errors={stats['errors']}"
        )

        return stats

    def get_order(self, order_id: str) -> Order | None:
        """
        Get an order by ID.

        Args:
            order_id: Order ID

        Returns:
            Order or None
        """
        return self._orders.get(order_id)

    def get_all_orders(self) -> list[Order]:
        """
        Get all orders.

        Returns:
            List of all orders
        """
        return list(self._orders.values())
