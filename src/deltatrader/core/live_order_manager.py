import asyncio
import uuid
from datetime import datetime
from typing import TYPE_CHECKING

from ..client.rest import RestClient
from ..models.order import Order, OrderStatus
from ..utils.integer_conversion import IntegerConverter
from ..utils.logger import logger
from ..utils.timing import get_timestamp_us
from .order_manager import OrderManager

if TYPE_CHECKING:
    from ..client.websocket import WebSocketClient


class LiveOrderManager(OrderManager):
    """Live order manager using REST API and WebSocket for real-time updates."""

    def __init__(
        self,
        rest_client: RestClient,
        converter: IntegerConverter,
        ws_client: "WebSocketClient | None" = None,
    ):
        """
        Initialize LiveOrderManager.

        Args:
            rest_client: REST client instance
            converter: Integer converter instance
            ws_client: WebSocket client for real-time order updates (optional)
        """
        super().__init__(converter)
        self.rest_client = rest_client
        self.ws_client = ws_client
        self._reconciliation_task: asyncio.Task | None = None
        self._reconciliation_interval = 300  # 5 minutes (as backup/risk check)
        self._running = False
        self._ws_subscribed = False

        # Statistics for monitoring
        self._ws_order_updates = 0
        self._ws_fill_updates = 0
        self._reconciliation_discrepancies = 0

        # Register WebSocket handlers if client provided
        if self.ws_client:
            self.ws_client.add_handler("orders", self._handle_order_update)
            self.ws_client.add_handler("fills", self._handle_fill_update)
            logger.info("WebSocket order update handlers registered")

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

    async def edit_order(
        self,
        client_order_id: str,
        new_size: int | None = None,
        new_price: int | None = None,
    ) -> Order | None:
        """
        Edit an existing order in-place using Delta Exchange's edit_order API.

        This modifies the order without cancelling it, preserving queue position.
        Delta Exchange supports editing order size and limit_price.

        Args:
            client_order_id: Order ID to edit
            new_size: New size (contracts), None to keep existing
            new_price: New price (integer), None to keep existing

        Returns:
            Updated order if successful, None otherwise
        """
        # Get the existing order
        old_order = self.get_order(client_order_id)
        if not old_order:
            logger.error(f"Order not found for edit: {client_order_id}")
            return None

        if not old_order.exchange_order_id:
            logger.error(f"No exchange order ID for {client_order_id}")
            return None

        # Use existing values if not specified
        size = new_size if new_size is not None else old_order.size
        price = new_price if new_price is not None else old_order.price

        # Check if anything actually changed
        if size == old_order.size and price == old_order.price:
            logger.info(f"No changes for order {client_order_id}, skipping edit")
            return old_order

        logger.info(
            f"Editing order {client_order_id}: "
            f"size {old_order.size}->{size}, price {old_order.price}->{price}"
        )

        try:
            # Convert to API format
            size_str = self.converter.integer_to_size(size)
            price_str = None
            if price is not None:
                price_str = self.converter.integer_to_price(old_order.symbol, price)

            # Edit order via REST API according to Delta Exchange spec
            response = await self.rest_client.edit_order(
                order_id=str(old_order.exchange_order_id),
                product_id=old_order.product_id,
                size=int(float(size_str)),
                limit_price=price_str,
            )

            # Update local order with response
            old_order.size = size
            old_order.price = price
            old_order.status = self._map_api_status(response.get("state", "open"))

            # Update filled size if provided
            if "unfilled_size" in response:
                unfilled = self.converter.size_to_integer(
                    str(response["unfilled_size"])
                )
                old_order.filled_size = size - unfilled

            logger.info(f"Order edited successfully: {client_order_id}")
            return old_order

        except Exception as e:
            logger.error(f"Failed to edit order {client_order_id}: {e}", exc_info=True)
            return None

    async def start_order_subscriptions(self) -> None:
        """
        Start receiving real-time order updates via WebSocket.

        This provides instant order status updates (10-500ms latency).
        """
        if not self.ws_client:
            logger.warning("No WebSocket client available for order subscriptions")
            return

        if self._ws_subscribed:
            logger.warning("Already subscribed to order updates")
            return

        try:
            # Subscribe to orders channel
            await self.ws_client.subscribe_orders()

            # Subscribe to fills channel for detailed fill information
            await self.ws_client.subscribe_fills()

            self._ws_subscribed = True
            logger.info(
                "WebSocket order subscriptions active (real-time updates enabled)"
            )

        except Exception as e:
            logger.error(f"Failed to start order subscriptions: {e}", exc_info=True)

    async def stop_order_subscriptions(self) -> None:
        """Stop receiving order updates via WebSocket."""
        self._ws_subscribed = False
        logger.info("WebSocket order subscriptions stopped")

    async def _handle_order_update(self, data: dict) -> None:
        """
        Handle real-time order update from WebSocket.

        This provides INSTANT order status updates vs 5-minute polling.
        Expected latency: 10-500ms from exchange action to this handler.

        Args:
            data: WebSocket message with order update
        """
        try:
            msg_type = data.get("type")
            logger.debug(f"Order update received: {msg_type} - {data}")

            # Extract order details (handle both nested and flat structures)
            order_data = data.get("order", data)

            # Get client order ID
            client_order_id = order_data.get("client_order_id")
            if not client_order_id:
                logger.warning(f"Order update missing client_order_id: {data}")
                return

            # Get or create order in cache
            if client_order_id not in self._orders:
                logger.info(
                    f"Received update for order not in cache: {client_order_id}, "
                    f"creating from WebSocket data"
                )
                # Try to create order from WebSocket data
                order = self._create_order_from_ws_data(order_data)
                if order:
                    self._orders[client_order_id] = order
                else:
                    logger.warning(f"Could not create order from WebSocket data")
                    return
            else:
                order = self._orders[client_order_id]

            # Update order status based on message type
            old_status = order.status

            if msg_type in ["order_created", "order_open"]:
                order.status = "open"

            elif msg_type == "order_closed":
                # Order closed - check if filled or cancelled
                filled_size = self.converter.size_to_integer(
                    str(order_data.get("size", 0))
                )
                if filled_size > 0:
                    order.status = "filled"
                    order.filled_size = filled_size
                    order.average_fill_price = self.converter.price_to_integer(
                        order.symbol, str(order_data.get("average_fill_price", 0))
                    )
                else:
                    order.status = "cancelled"

            elif msg_type == "order_cancelled":
                order.status = "cancelled"

            elif msg_type == "order_rejected":
                order.status = "rejected"

            # Update filled size and average fill price if provided
            if "size" in order_data and order.status == "filled":
                order.filled_size = self.converter.size_to_integer(
                    str(order_data.get("size"))
                )

            if "unfilled_size" in order_data:
                unfilled = self.converter.size_to_integer(
                    str(order_data.get("unfilled_size"))
                )
                order.filled_size = order.size - unfilled

            if "average_fill_price" in order_data and order_data["average_fill_price"]:
                order.average_fill_price = self.converter.price_to_integer(
                    order.symbol, str(order_data["average_fill_price"])
                )

            # Log status change
            if old_status != order.status:
                logger.info(
                    f"Order status update (WebSocket): {client_order_id} - "
                    f"{order.symbol} {order.side} {old_status} → {order.status}"
                )

                if order.status == "filled":
                    logger.info(
                        f"Order FILLED (WebSocket): {client_order_id} - "
                        f"{order.symbol} {order.side} {order.filled_size} @ {order.average_fill_price}"
                    )
                elif order.status == "cancelled":
                    logger.info(
                        f"Order CANCELLED (WebSocket): {client_order_id} - "
                        f"filled {order.filled_size}/{order.size}"
                    )

            self._ws_order_updates += 1

        except Exception as e:
            logger.error(f"Error handling order update: {e}", exc_info=True)

    async def _handle_fill_update(self, data: dict) -> None:
        """
        Handle real-time fill update from WebSocket.

        Provides detailed fill information including exact fill price and fees.

        Args:
            data: WebSocket message with fill details
        """
        try:
            logger.debug(f"Fill update received: {data}")

            # Extract fill details
            fill_data = data.get("fill", data)

            client_order_id = fill_data.get("client_order_id")
            if not client_order_id or client_order_id not in self._orders:
                logger.warning(f"Fill update for unknown order: {client_order_id}")
                return

            order = self._orders[client_order_id]

            # Update fill information
            fill_size = self.converter.size_to_integer(str(fill_data.get("size", 0)))
            fill_price = self.converter.price_to_integer(
                order.symbol, str(fill_data.get("price", 0))
            )

            # Update order filled size
            order.filled_size = min(order.filled_size + fill_size, order.size)

            # Update average fill price (weighted average)
            if order.average_fill_price:
                # Calculate weighted average
                total_value = (
                    order.average_fill_price * (order.filled_size - fill_size)
                    + fill_price * fill_size
                )
                order.average_fill_price = total_value // order.filled_size
            else:
                order.average_fill_price = fill_price

            # Update status
            if order.filled_size >= order.size:
                order.status = "filled"
            else:
                order.status = "partially_filled"

            logger.info(
                f"Fill update (WebSocket): {client_order_id} - "
                f"filled {fill_size} @ {fill_price}, "
                f"total {order.filled_size}/{order.size}, "
                f"avg price {order.average_fill_price}"
            )

            self._ws_fill_updates += 1

        except Exception as e:
            logger.error(f"Error handling fill update: {e}", exc_info=True)

    def _create_order_from_ws_data(self, order_data: dict) -> Order | None:
        """
        Create Order object from WebSocket data.

        Args:
            order_data: Order data from WebSocket message

        Returns:
            Order object or None if data is insufficient
        """
        try:
            # Extract required fields
            symbol = order_data.get("product", {}).get("symbol") or order_data.get(
                "symbol"
            )
            if not symbol:
                return None

            order = Order(
                symbol=symbol,
                side=order_data.get("side", "buy"),
                order_type=order_data.get("order_type", "limit_order"),
                size=self.converter.size_to_integer(str(order_data.get("size", 0))),
                price=self.converter.price_to_integer(
                    symbol, str(order_data.get("limit_price", 0))
                )
                if order_data.get("limit_price")
                else None,
            )

            # Set IDs and status
            order.client_order_id = order_data.get("client_order_id")
            order.exchange_order_id = order_data.get("id")
            order.status = self._map_api_status(order_data.get("state", "open"))

            # Set product ID
            product_id = order_data.get("product_id") or order_data.get(
                "product", {}
            ).get("id")
            if product_id:
                order.product_id = int(product_id)

            # Set filled size
            if "size" in order_data:
                order.filled_size = self.converter.size_to_integer(
                    str(order_data.get("size", 0))
                )

            if "unfilled_size" in order_data:
                unfilled = self.converter.size_to_integer(
                    str(order_data.get("unfilled_size", 0))
                )
                order.filled_size = order.size - unfilled

            # Set timestamp
            created_at = order_data.get("created_at")
            if created_at and isinstance(created_at, str):
                try:
                    dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                    order.timestamp = int(dt.timestamp() * 1_000_000)
                except (ValueError, AttributeError):
                    order.timestamp = get_timestamp_us()
            else:
                order.timestamp = get_timestamp_us()

            return order

        except Exception as e:
            logger.error(
                f"Error creating order from WebSocket data: {e}", exc_info=True
            )
            return None

    async def start_reconciliation(self) -> None:
        """
        Start periodic order reconciliation as a BACKUP/RISK CHECK.

        WebSocket provides primary updates (10-500ms latency).
        Reconciliation runs every 5 minutes to catch any missed updates
        and verify consistency between local state and exchange.
        """
        if self._running:
            logger.warning("Order reconciliation already running")
            return

        self._running = True
        self._reconciliation_task = asyncio.create_task(self._reconciliation_loop())

        if self.ws_client:
            logger.info(
                f"Started order reconciliation as BACKUP/RISK CHECK "
                f"(interval: {self._reconciliation_interval}s, "
                f"primary updates via WebSocket)"
            )
        else:
            logger.info(
                f"Started order reconciliation as PRIMARY method "
                f"(interval: {self._reconciliation_interval}s, "
                f"WebSocket not available)"
            )

    async def stop_reconciliation(self) -> None:
        """Stop periodic order reconciliation."""
        if not self._running:
            return

        logger.info("Stopping order reconciliation...")
        self._running = False

        if self._reconciliation_task:
            self._reconciliation_task.cancel()
            try:
                await self._reconciliation_task
            except asyncio.CancelledError:
                pass

        logger.info("Order reconciliation stopped")

    async def _reconciliation_loop(self) -> None:
        """
        Periodic reconciliation loop (BACKUP/RISK CHECK).

        When WebSocket is active, this serves as a safety net to catch
        any missed updates and verify state consistency.
        """
        try:
            while self._running:
                await asyncio.sleep(self._reconciliation_interval)

                try:
                    stats = await self.reconcile_orders()

                    # Log as risk check if WebSocket is active
                    if self.ws_client and self._ws_subscribed:
                        if (
                            stats["synced"] > 0
                            or stats["filled"] > 0
                            or stats["cancelled"] > 0
                        ):
                            logger.warning(
                                f"Reconciliation found discrepancies (WebSocket active): {stats}"
                            )
                            self._reconciliation_discrepancies += 1
                        else:
                            logger.debug(f"Reconciliation risk check passed: {stats}")
                    else:
                        logger.debug(f"Reconciliation stats: {stats}")

                    # Log statistics periodically
                    if self._ws_subscribed:
                        logger.info(
                            f"Order update statistics: "
                            f"WS_orders={self._ws_order_updates}, "
                            f"WS_fills={self._ws_fill_updates}, "
                            f"discrepancies={self._reconciliation_discrepancies}"
                        )

                except Exception as e:
                    logger.error(f"Error in reconciliation loop: {e}", exc_info=True)

        except asyncio.CancelledError:
            logger.debug("Reconciliation loop cancelled")

    def set_reconciliation_interval(self, interval: int) -> None:
        """
        Set the reconciliation interval.

        Args:
            interval: Interval in seconds (minimum 5)

        Note:
            When WebSocket is active, reconciliation serves as a backup/risk check.
            Recommended: 300 seconds (5 minutes) with WebSocket,
                        10-30 seconds without WebSocket.
        """
        if interval < 5:
            logger.warning("Reconciliation interval must be at least 5 seconds")
            interval = 5

        self._reconciliation_interval = interval

        if self.ws_client and self._ws_subscribed:
            logger.info(
                f"Reconciliation interval set to {interval}s (as backup/risk check)"
            )
        else:
            logger.info(
                f"Reconciliation interval set to {interval}s (as primary method)"
            )

    def get_statistics(self) -> dict:
        """
        Get order update statistics.

        Returns:
            Dictionary with update statistics
        """
        return {
            "ws_order_updates": self._ws_order_updates,
            "ws_fill_updates": self._ws_fill_updates,
            "reconciliation_discrepancies": self._reconciliation_discrepancies,
            "ws_subscribed": self._ws_subscribed,
            "reconciliation_interval": self._reconciliation_interval,
        }

    @staticmethod
    def _map_api_status(api_status: str) -> OrderStatus:
        """Map API status to internal status."""
        status_map: dict[str, OrderStatus] = {
            "open": "open",
            "pending": "pending",
            "closed": "filled",
            "cancelled": "cancelled",
            "rejected": "rejected",
        }
        return status_map.get(api_status, "pending")
