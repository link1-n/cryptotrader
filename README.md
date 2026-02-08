# DeltaTrader

A Python framework for algorithmic trading on Delta Exchange India futures market.

## Features

- **Async Architecture**: Built on asyncio for high-performance concurrent operations
- **Paper Trading**: Test strategies risk-free with simulated order execution
- **Live Trading**: Connect to Delta Exchange with full order management
- **Market Data**: Real-time orderbook and trade data via WebSocket
- **Strategy Framework**: Easy-to-use base class for implementing custom strategies
- **Type Safety**: Full type hints and Pydantic models for data validation
- **Integer Conversion**: Precise handling of prices and sizes using integer arithmetic

## Installation

### Requirements

- Python 3.10 or higher
- Delta Exchange API credentials (for live trading)

### Install from source

```bash
# Clone the repository
git clone <your-repo-url>
cd crypt

# Install using uv (recommended)
uv pip install -e .

# Or using pip
pip install -e .
```

## Configuration

Create a `.env` file in your project root:

```env
# Delta Exchange API Credentials
DELTA_API_KEY=your_api_key_here
DELTA_API_SECRET=your_api_secret_here

# Environment (optional, defaults to "production")
DELTA_ENVIRONMENT=testnet  # or "production" for live trading
```

## Quick Start

### Paper Trading Example

```python
import asyncio
from deltatrader import TradingEngine, Strategy, Config

class SimpleStrategy(Strategy):
    """Example strategy that monitors BTC-PERP market."""
    
    def __init__(self):
        super().__init__(
            name="SimpleStrategy",
            symbols=["BTCUSDT"]  # BTC perpetual futures
        )
    
    async def on_orderbook_update(self, symbol: str, orderbook):
        """Called when orderbook updates."""
        mid_price = orderbook.get_mid_price()
        spread = orderbook.get_spread()
        
        self.logger.info(
            f"{symbol} - Mid: {mid_price}, Spread: {spread}"
        )
    
    async def on_trade(self, symbol: str, trade):
        """Called on new trade."""
        self.logger.info(
            f"{symbol} - Trade: {trade.side} {trade.size} @ {trade.price}"
        )

async def main():
    # Initialize engine in demo mode (paper trading)
    engine = TradingEngine(demo_mode=True)
    
    try:
        # Initialize with specific symbols
        await engine.initialize(symbols=["BTCUSDT"])
        
        # Create and add strategy
        strategy = SimpleStrategy()
        await engine.add_strategy(strategy)
        
        # Start trading
        await engine.run()
        
    finally:
        await engine.stop()

if __name__ == "__main__":
    asyncio.run(main())
```

### Placing Orders

```python
from deltatrader.models import Order

# In your strategy's on_orderbook_update method:
async def on_orderbook_update(self, symbol: str, orderbook):
    # Create a limit buy order
    order = Order(
        symbol=symbol,
        side="buy",
        size=1,  # Number of contracts
        order_type="limit_order",
        price=orderbook.get_best_bid()  # Price at best bid
    )
    
    # Place the order
    result = await self.place_order(order)
    
    if result.status == "open":
        self.logger.info(f"Order placed: {result.order_id}")
    else:
        self.logger.error(f"Order failed: {result.status}")
```

### Cancelling Orders

```python
# Cancel a specific order
await self.cancel_order(order_id)

# Cancel all orders for a symbol
await self.cancel_all_orders(symbol="BTCUSDT")

# Cancel all orders
await self.cancel_all_orders()
```

## Architecture

### Core Components

- **TradingEngine**: Main orchestrator that manages all components
- **OrderManager**: Handles order placement, cancellation, and tracking
  - `LiveOrderManager`: For real trading via REST API
  - `PaperOrderManager`: For simulated trading
- **MarketDataManager**: Subscribes to and manages WebSocket market data
- **Strategy**: Base class for implementing trading strategies

### Market Data

The framework provides real-time market data through WebSocket connections:

- **Orderbook**: Level 2 orderbook with bid/ask levels
- **Trades**: Real-time trade feed
- **Products**: Instrument specifications and metadata

### Order Management

Orders are managed through an abstract `OrderManager` interface:

- Place orders (market, limit, stop-market, stop-limit)
- Cancel individual orders or all orders
- Track order status and fills
- Query open orders

## Project Structure

```
crypt/
├── src/
│   └── deltatrader/
│       ├── __init__.py          # Main package exports
│       ├── client/              # API clients
│       │   ├── auth.py          # Authentication
│       │   ├── rest.py          # REST API client
│       │   └── websocket.py     # WebSocket client
│       ├── core/                # Core trading components
│       │   ├── engine.py        # Trading engine
│       │   ├── market_data.py   # Market data manager
│       │   ├── order_manager.py # Order manager base
│       │   ├── live_order_manager.py    # Live trading
│       │   └── paper_order_manager.py   # Paper trading
│       ├── models/              # Data models
│       │   ├── order.py         # Order model
│       │   ├── orderbook.py     # Orderbook model
│       │   ├── product.py       # Product model
│       │   └── trade.py         # Trade model
│       ├── strategies/          # Strategy framework
│       │   ├── base.py          # Base strategy class
│       │   └── example_strategy.py
│       └── utils/               # Utilities
│           ├── config.py        # Configuration
│           ├── integer_conversion.py  # Price/size conversion
│           ├── logger.py        # Logging setup
│           └── timing.py        # Timestamp utilities
├── examples/                    # Example scripts
├── pyproject.toml              # Project configuration
└── README.md                   # This file
```

## Examples

See the `examples/` directory for more examples:

- `demo.py`: Basic market data demo
- `market_maker.py`: Simple market making strategy
- `test_order_placement.py`: Order placement examples
- `test_orderbook_parsing.py`: Orderbook parsing examples

## Development

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=deltatrader
```

### Code Style

The project uses:
- Type hints throughout
- Async/await for all I/O operations
- Pydantic for data validation
- Structured logging

## API Documentation

### TradingEngine

```python
engine = TradingEngine(demo_mode=True)
await engine.initialize(symbols=["BTCUSDT"])
await engine.add_strategy(strategy)
await engine.start()
await engine.stop()
```

### Strategy Base Class

Extend the `Strategy` class and override event handlers:

- `on_start()`: Called when strategy starts
- `on_stop()`: Called when strategy stops
- `on_orderbook_update(symbol, orderbook)`: Called on orderbook updates
- `on_trade(symbol, trade)`: Called on new trades
- `on_tick()`: Called periodically (every second)

### Order Model

```python
order = Order(
    symbol="BTCUSDT",
    side="buy",  # or "sell"
    size=1,  # Number of contracts
    order_type="limit_order",  # or "market_order"
    price=50000  # Optional for market orders
)
```

## Safety Features

1. **Paper Trading Mode**: Test strategies without risking real capital
2. **Order Limits**: Configurable limits on order size and frequency
3. **Error Handling**: Comprehensive error handling and logging
4. **Graceful Shutdown**: Cancels all orders on shutdown

## Logging

The framework uses structured logging with different levels:

- `DEBUG`: Detailed diagnostic information
- `INFO`: General information about operations
- `WARNING`: Warning messages for non-critical issues
- `ERROR`: Error messages for failures

Configure logging level in your code:

```python
from deltatrader.utils import logger
import logging

logger.setLevel(logging.DEBUG)
```

## Troubleshooting

### Circular Import Errors

If you encounter circular import errors, ensure you're importing from the correct modules:

```python
# Good
from deltatrader import TradingEngine, Strategy, Config

# Avoid
from deltatrader.core import TradingEngine  # May cause circular imports
```

### WebSocket Connection Issues

- Check your API credentials in `.env`
- Ensure you're using the correct environment (testnet vs production)
- Check network connectivity and firewall settings

### Order Placement Failures

- Verify product registration: `await engine.initialize(symbols=["BTCUSDT"])`
- Check order size and price are within exchange limits
- Ensure sufficient margin/balance (for live trading)

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Make your changes with tests
4. Submit a pull request

## License

[Your License Here]

## Disclaimer

This software is for educational purposes only. Trading cryptocurrencies carries significant risk. Always test strategies thoroughly in paper trading mode before using real capital. The authors are not responsible for any financial losses.

## Support

For questions and support:
- Open an issue on GitHub
- Check the examples directory
- Review the API documentation

## Changelog

### v0.1.0 (Current)

- Initial release
- Paper trading support
- Live trading via REST API
- Real-time market data via WebSocket
- Strategy framework
- Order management
- Integer-based price/size handling