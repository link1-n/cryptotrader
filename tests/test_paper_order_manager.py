"""Unit tests for PaperOrderManager."""

import asyncio
from datetime import datetime

import pytest

from deltatrader.core.paper_order_manager import PaperOrderManager
from deltatrader.models.order import Order
from deltatrader.utils.integer_conversion import IntegerConverter


class TestPaperOrderManager:
    """Test suite for PaperOrderManager."""

    @pytest.mark.asyncio
    async def test_initialization(self, converter: IntegerConverter):
        """Test PaperOrderManager initialization."""
        manager = PaperOrderManager(converter)

        assert manager.converter == converter
        assert manager._order_counter == 0
        assert manager._simulated_latency == 0.05
        assert len(manager._orders) == 0

    @pytest.mark.asyncio
    async def test_place_limit_order(
        self, paper_order_manager: PaperOrderManager, test_product
    ):
        """Test placing a limit order."""
        paper_order_manager.register_product(test_product)

        order = Order(
            symbol="BTCUSD",
            side="buy",
            order_type="limit_order",
            size=10,
            price=50000,
            product_id=84,
        )

        result = await paper_order_manager.place_order(order)

        # Verify order was placed successfully
        assert result.status == "open"
        assert result.exchange_order_id is not None
        assert result.client_order_id is not None
        assert result.timestamp > 0

        # Verify order is stored
        assert result.client_order_id in paper_order_manager._orders
        assert paper_order_manager._order_counter == 1

    @pytest.mark.asyncio
    async def test_place_market_order(
        self, paper_order_manager: PaperOrderManager, test_product
    ):
        """Test placing a market order."""
        paper_order_manager.register_product(test_product)

        order = Order(
            symbol="BTCUSD",
            side="sell",
            order_type="market_order",
            size=5,
            price=None,
            product_id=84,
        )

        result = await paper_order_manager.place_order(order)

        # Market orders should be accepted
        assert result.status == "open"
        assert result.client_order_id is not None

        # Wait for simulated fill
        await asyncio.sleep(0.2)

        # Check if order was filled (market orders auto-fill in paper trading)
        stored_order = paper_order_manager.get_order(result.client_order_id)
        assert stored_order.status == "filled"
        assert stored_order.filled_size == stored_order.size

    @pytest.mark.asyncio
    async def test_place_multiple_orders(
        self, paper_order_manager: PaperOrderManager, test_product
    ):
        """Test placing multiple orders."""
        paper_order_manager.register_product(test_product)

        orders = []
        for i in range(5):
            order = Order(
                symbol="BTCUSD",
                side="buy" if i % 2 == 0 else "sell",
                order_type="limit_order",
                size=i + 1,
                price=50000 + (i * 100),
                product_id=84,
            )
            result = await paper_order_manager.place_order(order)
            orders.append(result)

        # Verify all orders were placed
        assert len(orders) == 5
        assert paper_order_manager._order_counter == 5

        # Verify all have unique IDs
        order_ids = [o.client_order_id for o in orders]
        assert len(set(order_ids)) == 5

    @pytest.mark.asyncio
    async def test_cancel_order(
        self, paper_order_manager: PaperOrderManager, test_product
    ):
        """Test cancelling an order."""
        paper_order_manager.register_product(test_product)

        # Place an order
        order = Order(
            symbol="BTCUSD",
            side="buy",
            order_type="limit_order",
            size=10,
            price=50000,
            product_id=84,
        )
        placed_order = await paper_order_manager.place_order(order)

        # Cancel the order
        success = await paper_order_manager.cancel_order(placed_order.client_order_id)

        assert success is True

        # Verify order status is cancelled
        stored_order = paper_order_manager.get_order(placed_order.client_order_id)
        assert stored_order.status == "cancelled"

    @pytest.mark.asyncio
    async def test_cancel_nonexistent_order(
        self, paper_order_manager: PaperOrderManager
    ):
        """Test cancelling a non-existent order."""
        success = await paper_order_manager.cancel_order("nonexistent_id")

        assert success is False

    @pytest.mark.asyncio
    async def test_cancel_already_cancelled_order(
        self, paper_order_manager: PaperOrderManager, test_product
    ):
        """Test cancelling an already cancelled order."""
        paper_order_manager.register_product(test_product)

        # Place and cancel an order
        order = Order(
            symbol="BTCUSD",
            side="buy",
            order_type="limit_order",
            size=10,
            price=50000,
            product_id=84,
        )
        placed_order = await paper_order_manager.place_order(order)
        await paper_order_manager.cancel_order(placed_order.client_order_id)

        # Try to cancel again
        success = await paper_order_manager.cancel_order(placed_order.client_order_id)

        assert success is False

    @pytest.mark.asyncio
    async def test_cancel_all_orders(
        self, paper_order_manager: PaperOrderManager, test_product
    ):
        """Test cancelling all orders."""
        paper_order_manager.register_product(test_product)

        # Place multiple orders
        for i in range(5):
            order = Order(
                symbol="BTCUSD",
                side="buy",
                order_type="limit_order",
                size=i + 1,
                price=50000 + (i * 100),
                product_id=84,
            )
            await paper_order_manager.place_order(order)

        # Cancel all orders
        count = await paper_order_manager.cancel_all_orders()

        assert count == 5

        # Verify all orders are cancelled
        open_orders = await paper_order_manager.get_open_orders()
        assert len(open_orders) == 0

    @pytest.mark.asyncio
    async def test_cancel_all_orders_for_symbol(
        self, paper_order_manager: PaperOrderManager, test_product
    ):
        """Test cancelling all orders for a specific symbol."""
        paper_order_manager.register_product(test_product)

        # Create another test product
        from deltatrader.models.product import Product

        eth_product = Product(
            product_id=85,
            symbol="ETHUSD",
            description="Ethereum Perpetual Futures",
            contract_type="perpetual_futures",
            tick_size="0.5",
            contract_size="1",
            quoting_asset="USD",
            settling_asset="USDT",
        )
        paper_order_manager.register_product(eth_product)

        # Place BTC orders
        for i in range(3):
            order = Order(
                symbol="BTCUSD",
                side="buy",
                order_type="limit_order",
                size=i + 1,
                price=50000 + (i * 100),
                product_id=84,
            )
            await paper_order_manager.place_order(order)

        # Place ETH orders
        for i in range(2):
            order = Order(
                symbol="ETHUSD",
                side="buy",
                order_type="limit_order",
                size=i + 1,
                price=3000 + (i * 10),
                product_id=85,
            )
            await paper_order_manager.place_order(order)

        # Cancel only BTC orders
        count = await paper_order_manager.cancel_all_orders(symbol="BTCUSD")

        assert count == 3

        # Verify BTC orders are cancelled
        btc_orders = await paper_order_manager.get_open_orders("BTCUSD")
        assert len(btc_orders) == 0

        # Verify ETH orders are still open
        eth_orders = await paper_order_manager.get_open_orders("ETHUSD")
        assert len(eth_orders) == 2

    @pytest.mark.asyncio
    async def test_get_open_orders(
        self, paper_order_manager: PaperOrderManager, test_product
    ):
        """Test getting open orders."""
        paper_order_manager.register_product(test_product)

        # Place some orders
        for i in range(3):
            order = Order(
                symbol="BTCUSD",
                side="buy",
                order_type="limit_order",
                size=i + 1,
                price=50000 + (i * 100),
                product_id=84,
            )
            await paper_order_manager.place_order(order)

        # Get open orders
        open_orders = await paper_order_manager.get_open_orders()

        assert len(open_orders) == 3
        assert all(o.status in ["open", "pending"] for o in open_orders)

    @pytest.mark.asyncio
    async def test_get_open_orders_by_symbol(
        self, paper_order_manager: PaperOrderManager, test_product
    ):
        """Test getting open orders filtered by symbol."""
        paper_order_manager.register_product(test_product)

        # Place orders
        for i in range(3):
            order = Order(
                symbol="BTCUSD",
                side="buy",
                order_type="limit_order",
                size=i + 1,
                price=50000 + (i * 100),
                product_id=84,
            )
            await paper_order_manager.place_order(order)

        # Get open orders for BTCUSD
        btc_orders = await paper_order_manager.get_open_orders("BTCUSD")

        assert len(btc_orders) == 3
        assert all(o.symbol == "BTCUSD" for o in btc_orders)

    @pytest.mark.asyncio
    async def test_get_order(
        self, paper_order_manager: PaperOrderManager, test_product
    ):
        """Test getting a specific order."""
        paper_order_manager.register_product(test_product)

        # Place an order
        order = Order(
            symbol="BTCUSD",
            side="buy",
            order_type="limit_order",
            size=10,
            price=50000,
            product_id=84,
        )
        placed_order = await paper_order_manager.place_order(order)

        # Get the order
        retrieved_order = paper_order_manager.get_order(placed_order.client_order_id)

        assert retrieved_order is not None
        assert retrieved_order.client_order_id == placed_order.client_order_id
        assert retrieved_order.symbol == "BTCUSD"
        assert retrieved_order.size == 10

    @pytest.mark.asyncio
    async def test_simulate_fill(
        self, paper_order_manager: PaperOrderManager, test_product
    ):
        """Test manual fill simulation."""
        paper_order_manager.register_product(test_product)

        # Place an order
        order = Order(
            symbol="BTCUSD",
            side="buy",
            order_type="limit_order",
            size=10,
            price=50000,
            product_id=84,
        )
        placed_order = await paper_order_manager.place_order(order)

        # Manually simulate fill
        fill_price = 50000
        success = paper_order_manager.simulate_fill(
            placed_order.client_order_id, fill_price
        )

        assert success is True

        # Verify order is filled
        filled_order = paper_order_manager.get_order(placed_order.client_order_id)
        assert filled_order.status == "filled"
        assert filled_order.filled_size == filled_order.size
        assert filled_order.average_fill_price == fill_price

    @pytest.mark.asyncio
    async def test_edit_order(
        self, paper_order_manager: PaperOrderManager, test_product
    ):
        """Test editing an order."""
        paper_order_manager.register_product(test_product)

        # Place an order
        order = Order(
            symbol="BTCUSD",
            side="buy",
            order_type="limit_order",
            size=10,
            price=50000,
            product_id=84,
        )
        placed_order = await paper_order_manager.place_order(order)

        # Edit the order
        new_price = 51000
        new_size = 15
        edited_order = await paper_order_manager.edit_order(
            placed_order.client_order_id, new_size=new_size, new_price=new_price
        )

        assert edited_order is not None
        assert edited_order.price == new_price
        assert edited_order.size == new_size
        assert edited_order.client_order_id == placed_order.client_order_id

    @pytest.mark.asyncio
    async def test_edit_order_price_only(
        self, paper_order_manager: PaperOrderManager, test_product
    ):
        """Test editing only the price of an order."""
        paper_order_manager.register_product(test_product)

        # Place an order
        order = Order(
            symbol="BTCUSD",
            side="buy",
            order_type="limit_order",
            size=10,
            price=50000,
            product_id=84,
        )
        placed_order = await paper_order_manager.place_order(order)
        original_size = placed_order.size

        # Edit only price
        new_price = 51000
        edited_order = await paper_order_manager.edit_order(
            placed_order.client_order_id, new_price=new_price
        )

        assert edited_order is not None
        assert edited_order.price == new_price
        assert edited_order.size == original_size

    @pytest.mark.asyncio
    async def test_edit_order_no_changes(
        self, paper_order_manager: PaperOrderManager, test_product
    ):
        """Test editing an order with no actual changes."""
        paper_order_manager.register_product(test_product)

        # Place an order
        order = Order(
            symbol="BTCUSD",
            side="buy",
            order_type="limit_order",
            size=10,
            price=50000,
            product_id=84,
        )
        placed_order = await paper_order_manager.place_order(order)

        # Edit with same values
        edited_order = await paper_order_manager.edit_order(
            placed_order.client_order_id,
            new_size=placed_order.size,
            new_price=placed_order.price,
        )

        assert edited_order is not None
        assert edited_order == placed_order

    @pytest.mark.asyncio
    async def test_edit_nonexistent_order(self, paper_order_manager: PaperOrderManager):
        """Test editing a non-existent order."""
        edited_order = await paper_order_manager.edit_order(
            "nonexistent_id", new_price=50000
        )

        assert edited_order is None

    @pytest.mark.asyncio
    async def test_reconcile_orders(
        self, paper_order_manager: PaperOrderManager, test_product
    ):
        """Test order reconciliation."""
        paper_order_manager.register_product(test_product)

        # Place some orders
        orders = []
        for i in range(3):
            order = Order(
                symbol="BTCUSD",
                side="buy",
                order_type="limit_order",
                size=i + 1,
                price=50000 + (i * 100),
                product_id=84,
            )
            result = await paper_order_manager.place_order(order)
            orders.append(result)

        # Run reconciliation
        stats = await paper_order_manager.reconcile_orders()

        # In paper trading, all orders should be synced
        assert stats["synced"] == 3
        assert stats["errors"] == 0

    @pytest.mark.asyncio
    async def test_reconciliation_lifecycle(
        self, paper_order_manager: PaperOrderManager
    ):
        """Test starting and stopping reconciliation."""
        # Start reconciliation
        await paper_order_manager.start_reconciliation()
        assert paper_order_manager._running is True
        assert paper_order_manager._reconciliation_task is not None

        # Wait a bit
        await asyncio.sleep(0.5)

        # Stop reconciliation
        await paper_order_manager.stop_reconciliation()
        assert paper_order_manager._running is False

    @pytest.mark.asyncio
    async def test_set_reconciliation_interval(
        self, paper_order_manager: PaperOrderManager
    ):
        """Test setting reconciliation interval."""
        # Set valid interval
        paper_order_manager.set_reconciliation_interval(60)
        assert paper_order_manager._reconciliation_interval == 60

        # Set too low interval (should be clamped to 5)
        paper_order_manager.set_reconciliation_interval(2)
        assert paper_order_manager._reconciliation_interval == 5

    @pytest.mark.asyncio
    async def test_simulated_latency(
        self, paper_order_manager: PaperOrderManager, test_product
    ):
        """Test that simulated latency is applied."""
        paper_order_manager.register_product(test_product)

        order = Order(
            symbol="BTCUSD",
            side="buy",
            order_type="limit_order",
            size=10,
            price=50000,
            product_id=84,
        )

        # Measure time to place order
        import time

        start = time.time()
        await paper_order_manager.place_order(order)
        elapsed = time.time() - start

        # Should take at least the simulated latency
        assert elapsed >= paper_order_manager._simulated_latency

    @pytest.mark.asyncio
    async def test_get_all_orders(
        self, paper_order_manager: PaperOrderManager, test_product
    ):
        """Test getting all orders."""
        paper_order_manager.register_product(test_product)

        # Place multiple orders
        for i in range(5):
            order = Order(
                symbol="BTCUSD",
                side="buy",
                order_type="limit_order",
                size=i + 1,
                price=50000 + (i * 100),
                product_id=84,
            )
            await paper_order_manager.place_order(order)

        # Cancel one order
        all_orders = paper_order_manager.get_all_orders()
        await paper_order_manager.cancel_order(all_orders[0].client_order_id)

        # Get all orders (should include cancelled)
        all_orders = paper_order_manager.get_all_orders()

        assert len(all_orders) == 5
        assert sum(1 for o in all_orders if o.status == "cancelled") == 1
        assert sum(1 for o in all_orders if o.status == "open") == 4
