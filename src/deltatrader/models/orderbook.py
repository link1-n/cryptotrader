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
    # Store raw snapshot data for accurate checksum computation
    _raw_bids: list[tuple[str, str]] = field(
        default_factory=list
    )  # [(price_str, size_str), ...]
    _raw_asks: list[tuple[str, str]] = field(
        default_factory=list
    )  # [(price_str, size_str), ...]

    def update_from_snapshot(self, snapshot_data: dict, converter) -> None:
        """Update orderbook from l2_orderbook or l2_updates snapshot."""
        self.symbol = snapshot_data["symbol"]
        self.timestamp = int(snapshot_data.get("timestamp", 0))

        # Handle both sequence_no and last_sequence_no fields
        self.sequence_no = int(
            snapshot_data.get("sequence_no") or snapshot_data.get("last_sequence_no", 0)
        )

        # Convert bids - support both "buy" (l2_orderbook) and "bids" (l2_updates)
        self.bids = []
        self._raw_bids = []
        buy_levels = snapshot_data.get("buy") or snapshot_data.get("bids", [])
        for level in buy_levels:
            # l2_updates format: [price, size] array
            # l2_orderbook format: {"limit_price": "...", "size": ...} object
            if isinstance(level, list):
                price_str = str(level[0])
                size_str = str(level[1])
                price_int = converter.price_to_integer(self.symbol, price_str)
                size_int = converter.size_to_integer(size_str)
            else:
                price_str = level["limit_price"]
                size_str = str(level["size"])
                price_int = converter.price_to_integer(self.symbol, price_str)
                size_int = converter.size_to_integer(size_str)
            self.bids.append((price_int, size_int))
            self._raw_bids.append((price_str, size_str))

        # Convert asks - support both "sell" (l2_orderbook) and "asks" (l2_updates)
        self.asks = []
        self._raw_asks = []
        sell_levels = snapshot_data.get("sell") or snapshot_data.get("asks", [])
        for level in sell_levels:
            # l2_updates format: [price, size] array
            # l2_orderbook format: {"limit_price": "...", "size": ...} object
            if isinstance(level, list):
                price_str = str(level[0])
                size_str = str(level[1])
                price_int = converter.price_to_integer(self.symbol, price_str)
                size_int = converter.size_to_integer(size_str)
            else:
                price_str = level["limit_price"]
                size_str = str(level["size"])
                price_int = converter.price_to_integer(self.symbol, price_str)
                size_int = converter.size_to_integer(size_str)
            self.asks.append((price_int, size_int))
            self._raw_asks.append((price_str, size_str))

        # Sort: bids descending, asks ascending
        self.bids.sort(reverse=True, key=lambda x: x[0])
        self.asks.sort(key=lambda x: x[0])

        # Sort raw values to match
        self._raw_bids.sort(reverse=True, key=lambda x: float(x[0]))
        self._raw_asks.sort(key=lambda x: float(x[0]))

    def apply_update(self, update_data: dict, converter) -> bool:
        """
        Apply incremental l2_updates.
        Returns True if successful, False if sequence mismatch.
        """
        # Support both sequence_no and last_sequence_no field names
        new_seq = int(
            update_data.get("sequence_no") or update_data.get("last_sequence_no", 0)
        )

        # Check sequence continuity
        if self.sequence_no > 0 and new_seq != self.sequence_no + 1:
            return False

        self.sequence_no = new_seq
        self.timestamp = int(update_data.get("timestamp", 0))

        # Apply buy updates - support both "buy" (l2_orderbook) and "bids" (l2_updates)
        buy_updates = update_data.get("buy") or update_data.get("bids", [])
        for level in buy_updates:
            # l2_updates format: [price, size] array
            # l2_orderbook format: {"limit_price": "...", "size": ...} object
            if isinstance(level, list):
                price_str = str(level[0])
                size_str = str(level[1])
                price_int = converter.price_to_integer(self.symbol, price_str)
                size_int = converter.size_to_integer(size_str)
            else:
                price_str = level["limit_price"]
                size_str = str(level["size"])
                price_int = converter.price_to_integer(self.symbol, price_str)
                size_int = converter.size_to_integer(size_str)
            self._update_level(self.bids, price_int, size_int, reverse=True)
            self._update_raw_level(self._raw_bids, price_str, size_str, reverse=True)

        # Apply sell updates - support both "sell" (l2_orderbook) and "asks" (l2_updates)
        sell_updates = update_data.get("sell") or update_data.get("asks", [])
        for level in sell_updates:
            # l2_updates format: [price, size] array
            # l2_orderbook format: {"limit_price": "...", "size": ...} object
            if isinstance(level, list):
                price_str = str(level[0])
                size_str = str(level[1])
                price_int = converter.price_to_integer(self.symbol, price_str)
                size_int = converter.size_to_integer(size_str)
            else:
                price_str = level["limit_price"]
                size_str = str(level["size"])
                price_int = converter.price_to_integer(self.symbol, price_str)
                size_int = converter.size_to_integer(size_str)
            self._update_level(self.asks, price_int, size_int, reverse=False)
            self._update_raw_level(self._raw_asks, price_str, size_str, reverse=False)

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

    def _update_raw_level(
        self,
        levels: list[tuple[str, str]],
        price_str: str,
        size_str: str,
        reverse: bool,
    ) -> None:
        """Update or remove a raw price level (string format)."""
        price_float = float(price_str)

        # Find existing level
        for i, (p, _) in enumerate(levels):
            if float(p) == price_float:
                if float(size_str) == 0:
                    # Remove level
                    levels.pop(i)
                else:
                    # Update size
                    levels[i] = (price_str, size_str)
                return

        # Add new level if size > 0
        if float(size_str) > 0:
            levels.append((price_str, size_str))
            levels.sort(reverse=reverse, key=lambda x: float(x[0]))

    def validate_checksum(self, checksum: int, converter) -> bool:
        """
        Validate orderbook checksum using CRC32.
        Checksum computed over top 10 levels of bids and asks.
        """
        computed = self.compute_checksum(converter)
        return computed == checksum

    def compute_checksum(self, converter) -> int:
        """
        Compute CRC32 checksum over top 10 levels.

        Format for l2_updates: "ask_price:ask_size,ask_price:ask_size|bid_price:bid_size,bid_price:bid_size"
        - Asks first (ascending order)
        - Then bids (descending order)
        - Comma separators within each side
        - Pipe separator between asks and bids

        Uses raw string values from the server to ensure exact formatting match.
        """
        # Take top 10 bids and asks from raw values (already sorted)
        top_raw_asks = self._raw_asks[:10]
        top_raw_bids = self._raw_bids[:10]

        # Build ask strings (already in ascending order)
        ask_parts = [f"{price}:{size}" for price, size in top_raw_asks]

        # Build bid strings (already in descending order)
        bid_parts = [f"{price}:{size}" for price, size in top_raw_bids]

        # Combine: asks,asks,asks|bids,bids,bids
        checksum_string = ",".join(ask_parts) + "|" + ",".join(bid_parts)
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
        """Return orderbook representation with top 20 levels horizontally."""
        lines = [
            f"OrderBook(symbol={self.symbol}, seq={self.sequence_no}, timestamp={self.timestamp})"
        ]

        # Get top 20 levels
        top_asks = self.asks[:20]  # Lowest ask first
        top_bids = self.bids[:20]  # Highest bid first

        # Header: bid size, bid price, ask price, ask size
        lines.append(
            f"{'Bid Size':>14} {'Bid Price':>15} | {'Ask Price':<15} {'Ask Size':<14}"
        )
        lines.append("-" * 63)

        # Print side by side
        max_levels = max(len(top_asks), len(top_bids))
        for i in range(max_levels):
            bid_str = ""
            ask_str = ""

            if i < len(top_bids):
                bid_price, bid_size = top_bids[i]
                bid_str = f"{bid_size:>14} {bid_price:>15}"
            else:
                bid_str = " " * 30

            if i < len(top_asks):
                ask_price, ask_size = top_asks[i]
                ask_str = f"{ask_price:<15} {ask_size:<14}"
            else:
                ask_str = " " * 30

            lines.append(f"{bid_str} | {ask_str}")

        return "\n".join(lines)

    def __str__(self) -> str:
        """Return orderbook as string."""
        return self.__repr__()
