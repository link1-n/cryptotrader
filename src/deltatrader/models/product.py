"""Product model."""

from dataclasses import dataclass


@dataclass
class Product:
    """Represents a trading product (futures contract)."""

    product_id: int
    symbol: str
    description: str
    contract_type: str  # e.g., "perpetual_futures"
    tick_size: str  # Minimum price increment as string
    contract_size: str  # Size of 1 contract
    quoting_asset: str  # e.g., "USDT", "USD"
    settling_asset: str
    precision: int | None = None  # Decimal precision

    @classmethod
    def from_api(cls, data: dict) -> "Product":
        """Create Product from API response."""
        # Handle quoting_asset - can be string or dict
        quoting_asset_data = data.get("quoting_asset", "USD")
        if isinstance(quoting_asset_data, dict):
            quoting_asset = quoting_asset_data.get("symbol", "USD")
        else:
            quoting_asset = str(quoting_asset_data)

        # Handle settling_asset - can be string or dict
        settling_asset_data = data.get("settling_asset", "USDT")
        if isinstance(settling_asset_data, dict):
            settling_asset = settling_asset_data.get("symbol", "USDT")
        else:
            settling_asset = str(settling_asset_data)

        # Handle price_band - can be dict or missing
        price_band = data.get("price_band")
        precision = None
        if isinstance(price_band, dict):
            precision = price_band.get("precision")

        return cls(
            product_id=data["id"],
            symbol=data["symbol"],
            description=data.get("description", ""),
            contract_type=data.get("contract_type", ""),
            tick_size=str(data.get("tick_size", "0.01")),
            contract_size=str(data.get("contract_value", "1")),
            quoting_asset=quoting_asset,
            settling_asset=settling_asset,
            precision=precision,
        )
