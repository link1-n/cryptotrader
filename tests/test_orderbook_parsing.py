"""Unit tests for orderbook message parsing."""

import pytest

from deltatrader.models.orderbook import OrderBook
from deltatrader.models.product import Product
from deltatrader.utils.integer_conversion import IntegerConverter


class TestOrderbookParsing:
    """Test suite for orderbook message parsing."""

    @pytest.fixture
    def sample_l2_orderbook(self):
        """Sample l2_orderbook message from Delta Exchange."""
        return {
            "type": "l2_orderbook",
            "symbol": "BTCUSD",
            "timestamp": 1770465544198981,
            "last_sequence_no": 1088568,
            "product_id": 84,
            "spread": "14319.2",
            "buy": [
                {"depth": "1", "limit_price": "67924.0", "size": 1},
                {"depth": "1489", "limit_price": "66000.0", "size": 1488},
                {"depth": "1684", "limit_price": "61717.5", "size": 195},
            ],
            "sell": [
                {"depth": "18", "limit_price": "82243.2", "size": 18},
                {"depth": "24", "limit_price": "82944.8", "size": 6},
                {"depth": "74", "limit_price": "83166.8", "size": 50},
            ],
        }

    @pytest.fixture
    def btc_product(self):
        """Create BTC product fixture."""
        return Product(
            product_id=84,
            symbol="BTCUSD",
            description="Bitcoin Perpetual Futures",
            contract_type="perpetual_futures",
            tick_size="0.5",
            contract_size="1",
            quoting_asset="USD",
            settling_asset="USDT",
        )

    @pytest.fixture
    def btc_converter(self, btc_product):
        """Create converter with BTC product registered."""
        converter = IntegerConverter()
        converter.register_product(btc_product)
        return converter

    def test_parse_l2_orderbook_message(self, sample_l2_orderbook, btc_converter):
        """Test parsing of l2_orderbook messages."""
        orderbook = OrderBook(symbol="BTCUSD")

        # Parse the message
        orderbook.update_from_snapshot(sample_l2_orderbook, btc_converter)

        # Verify basic parsing
        assert orderbook.symbol == "BTCUSD"
        assert orderbook.timestamp == 1770465544198981
        assert orderbook.sequence_no == 1088568

    def test_orderbook_has_bids_and_asks(self, sample_l2_orderbook, btc_converter):
        """Test that bids and asks are populated."""
        orderbook = OrderBook(symbol="BTCUSD")
        orderbook.update_from_snapshot(sample_l2_orderbook, btc_converter)

        assert len(orderbook.bids) == 3
        assert len(orderbook.asks) == 3

    def test_best_bid_and_ask(self, sample_l2_orderbook, btc_converter):
        """Test best bid and ask extraction."""
        orderbook = OrderBook(symbol="BTCUSD")
        orderbook.update_from_snapshot(sample_l2_orderbook, btc_converter)

        best_bid = orderbook.get_best_bid()
        best_ask = orderbook.get_best_ask()

        # Verify values are positive
        assert best_bid[0] > 0
        assert best_bid[1] > 0
        assert best_ask[0] > 0
        assert best_ask[1] > 0

    def test_bid_less_than_ask(self, sample_l2_orderbook, btc_converter):
        """Test that bid is less than ask (sanity check)."""
        orderbook = OrderBook(symbol="BTCUSD")
        orderbook.update_from_snapshot(sample_l2_orderbook, btc_converter)

        best_bid = orderbook.get_best_bid()
        best_ask = orderbook.get_best_ask()

        assert best_bid[0] < best_ask[0]

    def test_mid_price_calculation(self, sample_l2_orderbook, btc_converter):
        """Test mid price calculation."""
        orderbook = OrderBook(symbol="BTCUSD")
        orderbook.update_from_snapshot(sample_l2_orderbook, btc_converter)

        mid_price = orderbook.get_mid_price()
        best_bid = orderbook.get_best_bid()
        best_ask = orderbook.get_best_ask()

        # Mid price should be between bid and ask
        assert best_bid[0] < mid_price < best_ask[0]

    def test_spread_calculation(self, sample_l2_orderbook, btc_converter):
        """Test spread calculation."""
        orderbook = OrderBook(symbol="BTCUSD")
        orderbook.update_from_snapshot(sample_l2_orderbook, btc_converter)

        spread = orderbook.get_spread()
        best_bid = orderbook.get_best_bid()
        best_ask = orderbook.get_best_ask()

        # Spread should be positive
        assert spread > 0
        # Spread should equal difference between bid and ask
        assert spread == best_ask[0] - best_bid[0]

    def test_sequence_number_parsing(self, sample_l2_orderbook, btc_converter):
        """Test that sequence number is parsed correctly."""
        orderbook = OrderBook(symbol="BTCUSD")
        orderbook.update_from_snapshot(sample_l2_orderbook, btc_converter)

        assert orderbook.sequence_no == 1088568

    def test_price_conversion_to_integer(self, sample_l2_orderbook, btc_converter):
        """Test that prices are converted to integers correctly."""
        orderbook = OrderBook(symbol="BTCUSD")
        orderbook.update_from_snapshot(sample_l2_orderbook, btc_converter)

        best_bid_price, _ = orderbook.get_best_bid()
        best_ask_price, _ = orderbook.get_best_ask()

        # Prices should be integers
        assert isinstance(best_bid_price, int)
        assert isinstance(best_ask_price, int)

        # Convert back to float for verification
        best_bid_float = float(btc_converter.integer_to_price("BTCUSD", best_bid_price))
        best_ask_float = float(btc_converter.integer_to_price("BTCUSD", best_ask_price))

        # Should be reasonable values (BTC price range)
        assert 1000 < best_bid_float < 1000000
        assert 1000 < best_ask_float < 1000000

    def test_size_conversion_to_integer(self, sample_l2_orderbook, btc_converter):
        """Test that sizes are converted to integers correctly."""
        orderbook = OrderBook(symbol="BTCUSD")
        orderbook.update_from_snapshot(sample_l2_orderbook, btc_converter)

        _, best_bid_size = orderbook.get_best_bid()
        _, best_ask_size = orderbook.get_best_ask()

        # Sizes should be integers
        assert isinstance(best_bid_size, int)
        assert isinstance(best_ask_size, int)

        # Sizes should be positive
        assert best_bid_size > 0
        assert best_ask_size > 0

    def test_orderbook_levels_count(self, sample_l2_orderbook, btc_converter):
        """Test that correct number of levels are parsed."""
        orderbook = OrderBook(symbol="BTCUSD")
        orderbook.update_from_snapshot(sample_l2_orderbook, btc_converter)

        # Should have 3 bid levels and 3 ask levels from sample data
        assert len(orderbook.bids) == 3
        assert len(orderbook.asks) == 3

    def test_orderbook_levels_sorted(self, sample_l2_orderbook, btc_converter):
        """Test that orderbook levels are sorted correctly."""
        orderbook = OrderBook(symbol="BTCUSD")
        orderbook.update_from_snapshot(sample_l2_orderbook, btc_converter)

        # Bids should be sorted descending (highest first)
        for i in range(len(orderbook.bids) - 1):
            assert orderbook.bids[i][0] >= orderbook.bids[i + 1][0]

        # Asks should be sorted ascending (lowest first)
        for i in range(len(orderbook.asks) - 1):
            assert orderbook.asks[i][0] <= orderbook.asks[i + 1][0]

    def test_empty_orderbook(self, btc_converter):
        """Test handling of empty orderbook."""
        orderbook = OrderBook(symbol="BTCUSD")

        empty_data = {
            "type": "l2_orderbook",
            "symbol": "BTCUSD",
            "timestamp": 1770465544198981,
            "last_sequence_no": 1,
            "buy": [],
            "sell": [],
        }

        orderbook.update_from_snapshot(empty_data, btc_converter)

        # Should handle gracefully
        assert len(orderbook.bids) == 0
        assert len(orderbook.asks) == 0

    def test_orderbook_with_missing_fields(self, btc_converter):
        """Test handling of orderbook with missing optional fields."""
        orderbook = OrderBook(symbol="BTCUSD")

        minimal_data = {
            "type": "l2_orderbook",
            "symbol": "BTCUSD",
            "timestamp": 1770465544198981,
            "last_sequence_no": 1,
            "buy": [
                {"limit_price": "50000.0", "size": 10},
            ],
            "sell": [
                {"limit_price": "50100.0", "size": 10},
            ],
        }

        # Should not raise exception
        orderbook.update_from_snapshot(minimal_data, btc_converter)

        assert len(orderbook.bids) == 1
        assert len(orderbook.asks) == 1

    def test_multiple_snapshot_updates(self, sample_l2_orderbook, btc_converter):
        """Test that multiple snapshot updates replace previous data."""
        orderbook = OrderBook(symbol="BTCUSD")

        # First snapshot
        orderbook.update_from_snapshot(sample_l2_orderbook, btc_converter)
        first_sequence = orderbook.sequence_no

        # Second snapshot with different sequence
        second_snapshot = sample_l2_orderbook.copy()
        second_snapshot["last_sequence_no"] = 1088600
        second_snapshot["buy"] = [
            {"limit_price": "68000.0", "size": 5},
        ]

        orderbook.update_from_snapshot(second_snapshot, btc_converter)

        # Should have new data
        assert orderbook.sequence_no == 1088600
        assert orderbook.sequence_no != first_sequence
        assert len(orderbook.bids) == 1
