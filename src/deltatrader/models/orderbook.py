"""Orderbook model with integer values."""

import zlib
from dataclasses import dataclass, field


@dataclass
class OrderBook:
    """Level 2 order book with integer prices and sizes."""

    symbol: str
    bids: list[tuple[int, int]] = field(
        default_factory=list
    )  # [(price_int, size_int), ...]
    asks: list[tuple[int, int]] = field(default_factory=list)
    timestamp: int = 0  # Microseconds
    sequence_no: int = 0

    def update_from_snapshot(self, snapshot_data: dict, converter) -> None:
        """Update orderbook from l2_orderbook or l2_updates snapshot."""
        self.symbol = snapshot_data["symbol"]
        self.timestamp = int(snapshot_data.get("timestamp", 0))

        # Handle both sequence_no and last_sequence_no fields
        self.sequence_no = int(
            snapshot_data.get("sequence_no") or snapshot_data.get("last_sequence_no", 0)
        )

        # Convert bids
        self.bids = []
        for level in snapshot_data.get("buy", []):
            price_int = converter.price_to_integer(self.symbol, level["limit_price"])
            size_int = converter.size_to_integer(level["size"])
            self.bids.append((price_int, size_int))

        # Convert asks
        self.asks = []
        for level in snapshot_data.get("sell", []):
            price_int = converter.price_to_integer(self.symbol, level["limit_price"])
            size_int = converter.size_to_integer(level["size"])
            self.asks.append((price_int, size_int))

        # Sort: bids descending, asks ascending
        self.bids.sort(reverse=True, key=lambda x: x[0])
        self.asks.sort(key=lambda x: x[0])

    def apply_update(self, update_data: dict, converter) -> bool:
        """
        Apply incremental l2_updates.
        Returns True if successful, False if sequence mismatch.
        """
        new_seq = int(update_data.get("sequence_no", 0))

        # Check sequence continuity
        if self.sequence_no > 0 and new_seq != self.sequence_no + 1:
            return False

        self.sequence_no = new_seq
        self.timestamp = int(update_data.get("timestamp", 0))

        # Apply buy updates
        for level in update_data.get("buy", []):
            price_int = converter.price_to_integer(self.symbol, level["limit_price"])
            size_int = converter.size_to_integer(level["size"])
            self._update_level(self.bids, price_int, size_int, reverse=True)

        # Apply sell updates
        for level in update_data.get("sell", []):
            price_int = converter.price_to_integer(self.symbol, level["limit_price"])
            size_int = converter.size_to_integer(level["size"])
            self._update_level(self.asks, price_int, size_int, reverse=False)

        return True

    def _update_level(
        self, levels: list[tuple[int, int]], price: int, size: int, reverse: bool
    ) -> None:
        """Update or remove a price level."""
        # Find existing level
        for i, (p, _) in enumerate(levels):
            if p == price:
                if size == 0:
                    # Remove level
                    levels.pop(i)
                else:
                    # Update size
                    levels[i] = (price, size)
                return

        # Add new level if size > 0
        if size > 0:
            levels.append((price, size))
            levels.sort(reverse=reverse, key=lambda x: x[0])

    def validate_checksum(self, checksum: int, converter) -> bool:
        """
        Validate orderbook checksum using CRC32.
        Checksum computed over top 10 levels of bids and asks.
        """
        computed = self.compute_checksum(converter)
        return computed == checksum

    def compute_checksum(self, converter) -> int:
        """Compute CRC32 checksum over top 10 levels."""
        # Take top 10 bids and asks
        top_bids = self.bids[:10]
        top_asks = self.asks[:10]

        # Build string: bid_price:bid_size:ask_price:ask_size:...
        parts = []
        max_levels = max(len(top_bids), len(top_asks))

        for i in range(max_levels):
            if i < len(top_bids):
                bid_price, bid_size = top_bids[i]
                parts.append(converter.integer_to_price(self.symbol, bid_price))
                parts.append(converter.integer_to_size(bid_size))

            if i < len(top_asks):
                ask_price, ask_size = top_asks[i]
                parts.append(converter.integer_to_price(self.symbol, ask_price))
                parts.append(converter.integer_to_size(ask_size))

        checksum_string = ":".join(parts)
        return zlib.crc32(checksum_string.encode()) & 0xFFFFFFFF

    def get_best_bid(self) -> tuple[int, int]:
        """Get best bid (highest price)."""
        return self.bids[0] if self.bids else (0, 0)

    def get_best_ask(self) -> tuple[int, int]:
        """Get best ask (lowest price)."""
        return self.asks[0] if self.asks else (0, 0)

    def get_mid_price(self) -> int:
        """Get mid price as integer."""
        if not self.bids or not self.asks:
            return 0
        return (self.bids[0][0] + self.asks[0][0]) // 2

    def get_spread(self) -> int:
        """Get spread as integer."""
        if not self.bids or not self.asks:
            return 0
        return self.asks[0][0] - self.bids[0][0]

    def __repr__(self) -> str:
        best_bid = self.get_best_bid()
        best_ask = self.get_best_ask()
        return f"OrderBook({self.symbol}, bid={best_bid}, ask={best_ask}, seq={self.sequence_no})"
