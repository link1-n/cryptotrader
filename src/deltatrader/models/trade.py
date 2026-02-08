"""Trade model."""

from dataclasses import dataclass
from typing import Literal


@dataclass
class Trade:
    """Represents a trade with integer values."""

    symbol: str
    trade_id: str
    price: int  # Price as integer (scaled)
    size: int  # Size as integer
    timestamp: int  # Microseconds
    side: Literal["buy", "sell"]

    @classmethod
    def from_api(cls, symbol: str, data: dict, converter) -> "Trade":
        """Create Trade from API response."""
        # Map buyer_role to side
        buyer_role = data.get("buyer_role", "")
        side = "buy" if buyer_role == "taker" else "sell"

        return cls(
            symbol=symbol,
            trade_id=str(data.get("id", data.get("trade_id", ""))),
            price=converter.price_to_integer(symbol, str(data["price"])),
            size=converter.size_to_integer(str(data["size"])),
            timestamp=int(data.get("timestamp", 0)),
            side=side,
        )

    def __repr__(self) -> str:
        return f"Trade({self.symbol}, {self.side}, price={self.price}, size={self.size}, ts={self.timestamp})"
