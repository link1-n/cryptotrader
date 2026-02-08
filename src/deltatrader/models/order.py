"""Order model."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

from ..utils.timing import get_timestamp_us

OrderSide = Literal["buy", "sell"]
OrderType = Literal["limit_order", "market_order"]
OrderStatus = Literal["pending", "open", "filled", "cancelled", "rejected"]


@dataclass
class Order:
    """Represents an order."""

    symbol: str
    side: OrderSide
    order_type: OrderType
    size: int  # Contract quantity as integer
    price: int | None = None  # Price as integer (None for market orders)

    # Internal tracking
    product_id: int | None = None  # Product ID from exchange
    client_order_id: str | None = None
    exchange_order_id: int | None = None
    status: OrderStatus = "pending"
    filled_size: int = 0
    average_fill_price: int | None = None
    timestamp: int = field(default_factory=get_timestamp_us)

    def to_api_payload(self, converter, product_id: int) -> dict:
        """Convert to Delta Exchange API payload."""
        payload = {
            "product_id": product_id,
            "size": converter.integer_to_size(self.size),
            "side": self.side,
            "order_type": self.order_type,
        }

        if self.price is not None:
            payload["limit_price"] = converter.integer_to_price(self.symbol, self.price)

        if self.client_order_id:
            payload["client_order_id"] = self.client_order_id

        return payload

    @classmethod
    def from_api(cls, data: dict, converter) -> "Order":
        """Create Order from API response."""
        symbol = data.get("product", {}).get("symbol", "")
        price_str = data.get("limit_price")

        # Parse timestamp - API returns ISO format string like '2026-02-07T12:22:51.882176Z'
        created_at = data.get("created_at")
        if created_at and isinstance(created_at, str):
            try:
                # Parse ISO format and convert to microseconds
                dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                timestamp = int(dt.timestamp() * 1_000_000)
            except (ValueError, AttributeError):
                timestamp = get_timestamp_us()
        else:
            timestamp = (
                created_at if isinstance(created_at, int) else get_timestamp_us()
            )

        return cls(
            symbol=symbol,
            side=data["side"],
            order_type=data["order_type"],
            size=converter.size_to_integer(str(data["unfilled_size"])),
            price=converter.price_to_integer(symbol, price_str) if price_str else None,
            client_order_id=data.get("client_order_id"),
            exchange_order_id=int(data["id"]),
            status=cls._map_status(data.get("state", "open")),
            filled_size=converter.size_to_integer(str(data.get("size", 0)))
            - converter.size_to_integer(str(data.get("unfilled_size", 0))),
            average_fill_price=converter.price_to_integer(
                symbol, str(data["average_fill_price"])
            )
            if data.get("average_fill_price")
            else None,
            timestamp=timestamp,
            product_id=data["product"]["id"],
        )

    @staticmethod
    def _map_status(api_status: str) -> OrderStatus:
        """Map API status to internal status."""
        status_map = {
            "open": "open",
            "pending": "pending",
            "closed": "filled",
            "cancelled": "cancelled",
            "rejected": "rejected",
        }
        return status_map.get(api_status, "pending")

    def __repr__(self) -> str:
        return (
            f"Order("
            f"symbol={self.symbol!r}, "
            f"side={self.side!r}, "
            f"order_type={self.order_type!r}, "
            f"size={self.size}, "
            f"price={self.price}, "
            f"product_id={self.product_id}, "
            f"client_order_id={self.client_order_id!r}, "
            f"exchange_order_id={self.exchange_order_id!r}, "
            f"client_order_id={self.client_order_id!r}, "
            f"status={self.status!r}, "
            f"filled_size={self.filled_size}, "
            f"average_fill_price={self.average_fill_price}, "
            f"timestamp={self.timestamp}"
            f")"
        )

    def __str__(self) -> str:
        """Human-readable string representation of the order."""
        price_str = f"{self.price}" if self.price is not None else "MARKET"
        avg_fill_str = (
            f"{self.average_fill_price}"
            if self.average_fill_price is not None
            else "N/A"
        )

        return (
            f"Order[{self.status.upper()}]: "
            f"{self.side.upper()} {self.size} {self.symbol} @ {price_str} "
            f"(type={self.order_type}, "
            f"filled={self.filled_size}/{self.size}, "
            f"avg_fill_price={avg_fill_str}, "
            f"exchange_order_id={self.exchange_order_id or 'N/A'}, "
            f"client_order_id={self.client_order_id or 'N/A'}, "
            f"product_id={self.product_id or 'N/A'}, "
            f"timestamp={self.timestamp})"
        )
