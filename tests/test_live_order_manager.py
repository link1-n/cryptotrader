"""Unit tests for LiveOrderManager."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from deltatrader.client.rest import RestClient
from deltatrader.core.live_order_manager import LiveOrderManager
from deltatrader.models.order import Order
from deltatrader.utils.integer_conversion import IntegerConverter


class TestLiveOrderManager:
    """Test suite for LiveOrderManager."""

    @pytest.mark.asyncio
    async def test_initialization(
        self, testnet_rest_client: RestClient, registered_converter: IntegerConverter
    ):
        """Test LiveOrderManager initialization."""
        manager = LiveOrderManager(testnet_rest_client, registered_converter)

        assert manager.converter == registered_converter
        assert manager.rest_client == testnet_rest_client
        assert len(manager._orders) == 0
        assert manager._reconciliation_interval == 30
        assert manager._running is False

    @pytest.mark.asyncio
    async def test_place_order_without_product(
        self, testnet_rest_client: RestClient, registered_converter: IntegerConverter
    ):
        """Test placing an order without registering the product."""
        manager = LiveOrderManager(testnet_rest_client, registered_converter)

        order = Order(
            symbol="UNKNOWN",
            side="buy",
            order_type="limit_order",
            size=10,
            price=50000,
        )

        result = await manager.place_order(order)

        # Should reject due to missing product
        assert result.status == "rejected"

    @pytest.mark.asyncio
    async def test_place_order_generates_client_order_id(
        self,
        testnet_rest_client: RestClient,
        registered_converter: IntegerConverter,
        test_product,
    ):
        """Test that client_order_id is generated if not provided."""
        manager = LiveOrderManager(testnet_rest_client, registered_converter)
        manager.register_product(test_product)

        # Mock the REST client response
        mock_response = {
            "id": "123",
            "state": "open",
            "created_at": "2024-01-01T00:00:00Z",
            "product": {"symbol": "BTCUSD", "id": 84},
            "side": "buy",
            "order_type": "limit_order",
            "size": 10,
            "unfilled_size": 10,
            "limit_price": "50000.0",
        }

        with patch.object(
            testnet_rest_client, "place_order", new_callable=AsyncMock
        ) as mock_place:
            mock_place.return_value = mock_response

            order = Order(
                symbol="BTCUSD",
                side="buy",
                order_type="limit_order",
                size=10,
                price=5000000,  # Integer price
            )

            result = await manager.place_order(order)

            # Should generate client_order_id
            assert result.client_order_id is not None
            assert len(result.client_order_id) == 32  # UUID hex

    @pytest.mark.asyncio
    async def test_place_order_preserves_client_order_id(
        self,
        testnet_rest_client: RestClient,
        registered_converter: IntegerConverter,
        test_product,
    ):
        """Test that provided client_order_id is preserved."""
        manager = LiveOrderManager(testnet_rest_client, registered_converter)
        manager.register_product(test_product)

        custom_id = "my_custom_order_id"

        mock_response = {
            "id": "123",
            "state": "open",
            "created_at": "2024-01-01T00:00:00Z",
            "product": {"symbol": "BTCUSD", "id": 84},
            "side": "buy",
            "order_type": "limit_order",
            "size": 10,
            "unfilled_size": 10,
            "limit_price": "50000.0",
            "client_order_id": custom_id,
        }

        with patch.object(
            testnet_rest_client, "place_order", new_callable=AsyncMock
        ) as mock_place:
            mock_place.return_value = mock_response

            order = Order(
                symbol="BTCUSD",
                side="buy",
                order_type="limit_order",
                size=10,
                price=5000000,
                client_order_id=custom_id,
            )

            result = await manager.place_order(order)

            assert result.client_order_id == custom_id

    @pytest.mark.asyncio
    async def test_place_order_stores_order(
        self,
        testnet_rest_client: RestClient,
        registered_converter: IntegerConverter,
        test_product,
    ):
        """Test that placed orders are stored in local cache."""
        manager = LiveOrderManager(testnet_rest_client, registered_converter)
        manager.register_product(test_product)

        mock_response = {
            "id": "123",
            "state": "open",
            "created_at": "2024-01-01T00:00:00Z",
            "product": {"symbol": "BTCUSD", "id": 84},
            "side": "buy",
            "order_type": "limit_order",
            "size": 10,
            "unfilled_size": 10,
            "limit_price": "50000.0",
            "client_order_id": "test_order",
        }

        with patch.object(
            testnet_rest_client, "place_order", new_callable=AsyncMock
        ) as mock_place:
            mock_place.return_value = mock_response

            order = Order(
                symbol="BTCUSD",
                side="buy",
                order_type="limit_order",
                size=10,
                price=5000000,
                client_order_id="test_order",
            )

            result = await manager.place_order(order)

            # Order should be stored
            assert "test_order" in manager._orders
            assert manager._orders["test_order"] == result

    @pytest.mark.asyncio
    async def test_place_order_handles_exception(
        self,
        testnet_rest_client: RestClient,
        registered_converter: IntegerConverter,
        test_product,
    ):
        """Test that exceptions during order placement are handled."""
        manager = LiveOrderManager(testnet_rest_client, registered_converter)
        manager.register_product(test_product)

        with patch.object(
            testnet_rest_client, "place_order", new_callable=AsyncMock
        ) as mock_place:
            mock_place.side_effect = Exception("API Error")

            order = Order(
                symbol="BTCUSD",
                side="buy",
                order_type="limit_order",
                size=10,
                price=5000000,
            )

            result = await manager.place_order(order)

            # Should be rejected
            assert result.status == "rejected"

    @pytest.mark.asyncio
    async def test_cancel_order_success(
        self,
        testnet_rest_client: RestClient,
        registered_converter: IntegerConverter,
        test_product,
    ):
        """Test successfully cancelling an order."""
        manager = LiveOrderManager(testnet_rest_client, registered_converter)
        manager.register_product(test_product)

        # Add a mock order to the manager
        order = Order(
            symbol="BTCUSD",
            side="buy",
            order_type="limit_order",
            size=10,
            price=5000000,
            client_order_id="test_order",
            product_id=84,
            status="open",
        )
        manager._orders["test_order"] = order

        with patch.object(
            testnet_rest_client, "cancel_order", new_callable=AsyncMock
        ) as mock_cancel:
            mock_cancel.return_value = {}

            success = await manager.cancel_order("test_order")

            assert success is True
            assert manager._orders["test_order"].status == "cancelled"

    @pytest.mark.asyncio
    async def test_cancel_order_not_found(
        self,
        testnet_rest_client: RestClient,
        registered_converter: IntegerConverter,
    ):
        """Test cancelling a non-existent order."""
        manager = LiveOrderManager(testnet_rest_client, registered_converter)

        with patch.object(
            testnet_rest_client, "cancel_order", new_callable=AsyncMock
        ) as mock_cancel:
            mock_cancel.return_value = {}

            success = await manager.cancel_order("nonexistent")

            # Should return False due to missing product_id
            assert success is False

    @pytest.mark.asyncio
    async def test_cancel_order_404_handled(
        self,
        testnet_rest_client: RestClient,
        registered_converter: IntegerConverter,
        test_product,
    ):
        """Test that 404 errors during cancellation are handled gracefully."""
        manager = LiveOrderManager(testnet_rest_client, registered_converter)
        manager.register_product(test_product)

        order = Order(
            symbol="BTCUSD",
            side="buy",
            order_type="limit_order",
            size=10,
            price=5000000,
            client_order_id="test_order",
            product_id=84,
            status="open",
        )
        manager._orders["test_order"] = order

        with patch.object(
            testnet_rest_client, "cancel_order", new_callable=AsyncMock
        ) as mock_cancel:
            mock_cancel.side_effect = Exception("404")

            success = await manager.cancel_order("test_order")

            # 404 should be treated as success
            assert success is True
            assert manager._orders["test_order"].status == "cancelled"

    @pytest.mark.asyncio
    async def test_cancel_all_orders(
        self,
        testnet_rest_client: RestClient,
        registered_converter: IntegerConverter,
        test_product,
    ):
        """Test cancelling all orders."""
        manager = LiveOrderManager(testnet_rest_client, registered_converter)
        manager.register_product(test_product)

        # Add multiple orders
        for i in range(3):
            order = Order(
                symbol="BTCUSD",
                side="buy",
                order_type="limit_order",
                size=10,
                price=5000000 + (i * 100),
                client_order_id=f"order_{i}",
                status="open",
            )
            manager._orders[f"order_{i}"] = order

        with patch.object(
            testnet_rest_client, "cancel_all_orders", new_callable=AsyncMock
        ) as mock_cancel_all:
            mock_cancel_all.return_value = {}

            count = await manager.cancel_all_orders()

            assert count == 3
            assert all(
                o.status == "cancelled"
                for o in manager._orders.values()
                if o.client_order_id.startswith("order_")
            )

    @pytest.mark.asyncio
    async def test_get_open_orders(
        self,
        testnet_rest_client: RestClient,
        registered_converter: IntegerConverter,
        test_product,
    ):
        """Test getting open orders from exchange."""
        manager = LiveOrderManager(testnet_rest_client, registered_converter)
        manager.register_product(test_product)

        mock_orders = [
            {
                "id": "123",
                "state": "open",
                "created_at": "2024-01-01T00:00:00Z",
                "product": {"symbol": "BTCUSD", "id": 84},
                "side": "buy",
                "order_type": "limit_order",
                "size": 10,
                "unfilled_size": 10,
                "limit_price": "50000.0",
                "client_order_id": "order_1",
            },
            {
                "id": "124",
                "state": "open",
                "created_at": "2024-01-01T00:00:00Z",
                "product": {"symbol": "BTCUSD", "id": 84},
                "side": "sell",
                "order_type": "limit_order",
                "size": 5,
                "unfilled_size": 5,
                "limit_price": "51000.0",
                "client_order_id": "order_2",
            },
        ]

        with patch.object(
            testnet_rest_client, "get_open_orders", new_callable=AsyncMock
        ) as mock_get:
            mock_get.return_value = mock_orders

            orders = await manager.get_open_orders()

            assert len(orders) == 2
            assert orders[0].client_order_id == "order_1"
            assert orders[1].client_order_id == "order_2"

    @pytest.mark.asyncio
    async def test_edit_order(
        self,
        testnet_rest_client: RestClient,
        registered_converter: IntegerConverter,
        test_product,
    ):
        """Test editing an order."""
        manager = LiveOrderManager(testnet_rest_client, registered_converter)
        manager.register_product(test_product)

        # Add order to manager
        order = Order(
            symbol="BTCUSD",
            side="buy",
            order_type="limit_order",
            size=10,
            price=5000000,
            client_order_id="test_order",
            exchange_order_id=123,
            product_id=84,
            status="open",
        )
        manager._orders["test_order"] = order

        mock_response = {
            "id": "123",
            "state": "open",
            "size": 15,
            "unfilled_size": 15,
            "limit_price": "51000.0",
        }

        with patch.object(
            testnet_rest_client, "edit_order", new_callable=AsyncMock
        ) as mock_edit:
            mock_edit.return_value = mock_response

            edited = await manager.edit_order(
                "test_order", new_size=15, new_price=5100000
            )

            assert edited is not None
            assert edited.size == 15
            assert edited.price == 5100000

    @pytest.mark.asyncio
    async def test_reconcile_orders(
        self,
        testnet_rest_client: RestClient,
        registered_converter: IntegerConverter,
        test_product,
    ):
        """Test order reconciliation."""
        manager = LiveOrderManager(testnet_rest_client, registered_converter)
        manager.register_product(test_product)

        # Add local orders
        manager._orders["order_1"] = Order(
            symbol="BTCUSD",
            side="buy",
            order_type="limit_order",
            size=10,
            price=5000000,
            client_order_id="order_1",
            status="open",
        )
        manager._orders["order_2"] = Order(
            symbol="BTCUSD",
            side="buy",
            order_type="limit_order",
            size=10,
            price=5000000,
            client_order_id="order_2",
            status="open",
        )

        # Mock exchange only has order_1
        mock_orders = [
            {
                "id": "123",
                "state": "open",
                "created_at": "2024-01-01T00:00:00Z",
                "product": {"symbol": "BTCUSD", "id": 84},
                "side": "buy",
                "order_type": "limit_order",
                "size": 10,
                "unfilled_size": 10,
                "limit_price": "50000.0",
                "client_order_id": "order_1",
            }
        ]

        with patch.object(
            testnet_rest_client, "get_open_orders", new_callable=AsyncMock
        ) as mock_get:
            mock_get.return_value = mock_orders

            stats = await manager.reconcile_orders()

            # order_1 should be synced, order_2 should be cancelled
            assert stats["synced"] == 1
            assert stats["cancelled"] == 1
            assert manager._orders["order_2"].status == "cancelled"

    @pytest.mark.asyncio
    async def test_reconciliation_lifecycle(
        self,
        testnet_rest_client: RestClient,
        registered_converter: IntegerConverter,
    ):
        """Test starting and stopping reconciliation."""
        manager = LiveOrderManager(testnet_rest_client, registered_converter)

        # Start reconciliation
        await manager.start_reconciliation()
        assert manager._running is True
        assert manager._reconciliation_task is not None

        # Wait briefly
        await asyncio.sleep(0.1)

        # Stop reconciliation
        await manager.stop_reconciliation()
        assert manager._running is False

    @pytest.mark.asyncio
    async def test_set_reconciliation_interval(
        self,
        testnet_rest_client: RestClient,
        registered_converter: IntegerConverter,
    ):
        """Test setting reconciliation interval."""
        manager = LiveOrderManager(testnet_rest_client, registered_converter)

        # Set valid interval
        manager.set_reconciliation_interval(60)
        assert manager._reconciliation_interval == 60

        # Set too low (should clamp to 5)
        manager.set_reconciliation_interval(2)
        assert manager._reconciliation_interval == 5

    @pytest.mark.asyncio
    async def test_map_api_status(self):
        """Test status mapping from API to internal format."""
        assert LiveOrderManager._map_api_status("open") == "open"
        assert LiveOrderManager._map_api_status("pending") == "pending"
        assert LiveOrderManager._map_api_status("closed") == "filled"
        assert LiveOrderManager._map_api_status("cancelled") == "cancelled"
        assert LiveOrderManager._map_api_status("rejected") == "rejected"
        assert LiveOrderManager._map_api_status("unknown") == "pending"
