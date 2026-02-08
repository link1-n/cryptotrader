"""Integer conversion utilities for precise decimal handling."""

from decimal import Decimal

from ..models.product import Product


class IntegerConverter:
    """Converts decimal prices/sizes to integers and back."""

    def __init__(self):
        self._product_scales: dict[str, int] = {}
        self._product_tick_sizes: dict[str, int] = {}

    def register_product(self, product: Product) -> None:
        """Register a product for conversion."""
        # Calculate scale factor from tick_size
        # tick_size is the minimum price increment
        # Example: tick_size="0.01" -> scale=100 (multiply by 100)
        tick_size_decimal = Decimal(str(product.tick_size))

        # Find number of decimal places
        scale = 0
        temp = tick_size_decimal
        while temp != int(temp):
            temp *= 10
            scale += 1

        self._product_scales[product.symbol] = 10**scale

        # Store integer representation of tick_size
        self._product_tick_sizes[product.symbol] = int(temp)

    def get_scale(self, symbol: str) -> int:
        """Get scale factor for a symbol."""
        return self._product_scales.get(symbol, 100000000)  # Default 8 decimals

    def price_to_integer(self, symbol: str, price: str) -> int:
        """Convert price string to integer."""
        scale = self.get_scale(symbol)
        decimal_price = Decimal(price)
        return int(decimal_price * scale)

    def size_to_integer(self, size) -> int:
        """Convert size string or int to integer (contracts are usually integers already)."""
        # Sizes in futures are typically integer contract counts
        # But some APIs send them as strings, others as ints
        if isinstance(size, int):
            return size

        size_str = str(size)
        if "." in size_str:
            # Handle decimal sizes if present
            decimal_size = Decimal(size_str)
            return int(decimal_size * 100000000)  # 8 decimal precision
        return int(size_str)

    def integer_to_price(self, symbol: str, price_int: int) -> str:
        """Convert integer price back to decimal string."""
        scale = self.get_scale(symbol)
        decimal_price = Decimal(price_int) / Decimal(scale)
        return str(decimal_price)

    def integer_to_size(self, size_int: int) -> str:
        """Convert integer size back to string."""
        # If we used 8 decimal precision for sizes
        if size_int > 1000000:
            decimal_size = Decimal(size_int) / Decimal(100000000)
            return str(decimal_size)
        return str(size_int)

    def normalize_price(self, symbol: str, price_int: int) -> int:
        """Normalize price to nearest valid tick."""
        tick_size = self._product_tick_sizes.get(symbol, 1)
        if tick_size == 1:
            return price_int

        # Round to nearest tick
        remainder = price_int % tick_size
        if remainder == 0:
            return price_int

        # Round to nearest
        if remainder >= tick_size // 2:
            return price_int + (tick_size - remainder)
        return price_int - remainder

    def set_scale(self, symbol: str, scale: int, tick_size_int: int = 1) -> None:
        """Manually set scale for a symbol."""
        self._product_scales[symbol] = scale
        self._product_tick_sizes[symbol] = tick_size_int
