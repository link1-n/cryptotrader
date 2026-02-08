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
    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        """
        Get all open orders.

        Args:
            symbol: Optional symbol to filter by

        Returns:
            List of open orders
        """
        pass

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
