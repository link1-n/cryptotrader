import uuid
from datetime import datetime

from ..client.rest import RestClient
from ..models.order import Order, OrderStatus
from ..utils.integer_conversion import IntegerConverter
from ..utils.logger import logger
from ..utils.timing import get_timestamp_us
from .order_manager import OrderManager


class LiveOrderManager(OrderManager):
    """Live order manager using REST API."""

    def __init__(self, rest_client: RestClient, converter: IntegerConverter):
        """
        Initialize LiveOrderManager.

        Args:
            rest_client: REST client instance
            converter: Integer converter instance
        """
        super().__init__(converter)
        self.rest_client = rest_client

    async def place_order(self, order: Order) -> Order:
        """
        Place a new order via REST API.

        Args:
            order: Order to place

        Returns:
            Updated order with ID and status
        """
        try:
            # Get product ID
            product_id = self.get_product_id(order.symbol)
            if product_id is None:
                logger.error(f"Product not registered: {order.symbol}")
                order.status = "rejected"
                return order

            # Generate client order ID if not set (max 32 chars for Delta Exchange)
            if not order.client_order_id:
                # Use UUID hex (32 chars) instead of full UUID string (36 chars with hyphens)
                order.client_order_id = uuid.uuid4().hex

            # Convert to API payload
            size_str = self.converter.integer_to_size(order.size)
            price_str = None
            if order.price is not None:
                price_str = self.converter.integer_to_price(order.symbol, order.price)

            # Place order
            response = await self.rest_client.place_order(
                product_id=product_id,
                size=int(float(size_str)),  # API expects integer contract count
                side=order.side,
                order_type=order.order_type,
                limit_price=price_str,
                client_order_id=order.client_order_id,
            )

            # Update order with response
            order.exchange_order_id = int(response.get("id", ""))
            order.product_id = product_id
            order.status = self._map_api_status(response.get("state", "open"))

            # Parse timestamp - API returns ISO format string like '2026-02-07T12:22:51.882176Z'
            created_at = response.get("created_at")
            if created_at and isinstance(created_at, str):
                try:
                    # Parse ISO format and convert to microseconds
                    dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                    order.timestamp = int(dt.timestamp() * 1_000_000)
                except (ValueError, AttributeError):
                    order.timestamp = get_timestamp_us()
            else:
                order.timestamp = get_timestamp_us()

            # Store order
            if order.client_order_id:
                self._orders[order.client_order_id] = order

            logger.info(
                f"Order placed: {order.symbol} {order.side} {order.size} @ {order.price} - Exchange Order ID: {order.exchange_order_id}"
            )
            return order

        except Exception as e:
            logger.error(f"Failed to place order: {e}", exc_info=True)
            order.status = "rejected"
            return order

    async def cancel_order(self, client_order_id: str) -> bool:
        """
        Cancel an order via REST API.

        Args:
            order_id: Order ID to cancel

        Returns:
            True if successful
        """
        try:
            # Get product_id from stored order if available
            product_id: int | None = None

            if client_order_id in self._orders:
                product_id = self._orders[client_order_id].product_id
                product_symbol = self._orders[client_order_id].symbol
                logger.debug(
                    f"CANCELLING ORDER -> ClientOrderID: {client_order_id}, ProductId: {product_id}, ProductSymbol: {product_symbol}"
                )

            if not product_id:
                logger.error("PRODUCT ID NONE")
                return False

            await self.rest_client.cancel_order(client_order_id, product_id=product_id)

            # Update local order status
            if client_order_id in self._orders:
                self._orders[client_order_id].status = "cancelled"

            logger.info(f"Order cancelled: {client_order_id}")
            return True

        except Exception as e:
            # 404 means order doesn't exist (already cancelled/filled)
            if "404" in str(e):
                logger.info(
                    f"Order {client_order_id} already cancelled or filled (404)"
                )
                if client_order_id in self._orders:
                    self._orders[client_order_id].status = "cancelled"
                return True

            logger.error(f"Failed to cancel order {client_order_id}: {e}")
            return False

    async def cancel_all_orders(self, symbol: str | None = None) -> int:
        """
        Cancel all orders via REST API.

        Args:
            symbol: Optional symbol to filter by

        Returns:
            Number of orders cancelled
        """
        try:
            product_id = None
            if symbol:
                product_id = self.get_product_id(symbol)

            await self.rest_client.cancel_all_orders(product_id=product_id)

            # Update local order statuses
            count = 0
            for order in self._orders.values():
                if symbol is None or order.symbol == symbol:
                    if order.status == "open" or order.status == "pending":
                        order.status = "cancelled"
                        count += 1

            logger.info(f"Cancelled {count} orders for {symbol or 'all symbols'}")
            return count

        except Exception as e:
            logger.error(f"Failed to cancel all orders: {e}")
            return 0

    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        """
        Get open orders from REST API.

        Args:
            symbol: Optional symbol to filter by

        Returns:
            List of open orders
        """
        try:
            product_id = None
            if symbol:
                product_id = self.get_product_id(symbol)

            response = await self.rest_client.get_open_orders(product_id=product_id)

            # Parse orders
            orders = []
            for order_data in response:
                try:
                    logger.debug(f"GET OPEN ORDERS RESPONSE -> {order_data}")
                    order = Order.from_api(order_data, self.converter)
                    if not order.client_order_id:
                        continue
                    self._orders[order.client_order_id] = order
                    orders.append(order)
                except Exception as e:
                    logger.warning(f"Failed to parse order: {e}")

            return orders

        except Exception as e:
            logger.error(f"Failed to get open orders: {e}")
            return []

    @staticmethod
    def _map_api_status(api_status: str) -> OrderStatus:
        """Map API status to internal status."""
        status_map = {
            "open": "open",
            "pending": "pending",
            "closed": "filled",
            "cancelled": "cancelled",
            "rejected": "rejected",
        }
        return status_map.get(api_status, "pending")
