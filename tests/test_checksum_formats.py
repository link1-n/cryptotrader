"""Test checksum computation with various data formats."""

import zlib

import pytest

from deltatrader.models.orderbook import OrderBook
from deltatrader.utils.integer_conversion import IntegerConverter


class TestChecksumFormats:
    """Test checksum computation with different data formats."""

    @pytest.fixture
    def converter(self):
        """Create a converter instance."""
        return IntegerConverter()

    def test_checksum_with_l2_updates_array_format(self, converter):
        """Test checksum with l2_updates array format [price, size]."""
        orderbook = OrderBook(symbol="BTCUSD")

        # Simulate l2_updates snapshot with array format
        snapshot_data = {
            "symbol": "BTCUSD",
            "timestamp": 1234567890,
            "sequence_no": 100,
            "bids": [
                ["71257.0", "3369"],
                ["71256.5", "4121"],
                ["71256.0", "2500"],
            ],
            "asks": [
                ["71258.0", "1500"],
                ["71258.5", "2000"],
                ["71259.0", "3500"],
            ],
        }

        orderbook.update_from_snapshot(snapshot_data, converter)

        # Expected checksum string format
        expected_string = "71258.0:1500,71258.5:2000,71259.0:3500|71257.0:3369,71256.5:4121,71256.0:2500"
        expected_checksum = zlib.crc32(expected_string.encode()) & 0xFFFFFFFF

        # Verify our computation matches
        computed = orderbook.compute_checksum(converter)
        assert computed == expected_checksum

        # Verify raw values are preserved correctly
        assert orderbook._raw_asks == [
            ("71258.0", "1500"),
            ("71258.5", "2000"),
            ("71259.0", "3500"),
        ]
        assert orderbook._raw_bids == [
            ("71257.0", "3369"),
            ("71256.5", "4121"),
            ("71256.0", "2500"),
        ]

    def test_checksum_with_l2_orderbook_object_format(self, converter):
        """Test checksum with l2_orderbook object format."""
        orderbook = OrderBook(symbol="BTCUSD")

        # Simulate l2_orderbook snapshot with object format
        snapshot_data = {
            "symbol": "BTCUSD",
            "timestamp": 1234567890,
            "last_sequence_no": 100,
            "buy": [
                {"limit_price": "50000.0", "size": "1.5"},
                {"limit_price": "49999.5", "size": "2.0"},
            ],
            "sell": [
                {"limit_price": "50000.5", "size": "1.0"},
                {"limit_price": "50001.0", "size": "1.5"},
            ],
        }

        orderbook.update_from_snapshot(snapshot_data, converter)

        # Expected checksum string format
        expected_string = "50000.5:1.0,50001.0:1.5|50000.0:1.5,49999.5:2.0"
        expected_checksum = zlib.crc32(expected_string.encode()) & 0xFFFFFFFF

        # Verify our computation matches
        computed = orderbook.compute_checksum(converter)
        assert computed == expected_checksum

    def test_checksum_updates_after_incremental_changes(self, converter):
        """Test that checksum updates correctly after incremental changes."""
        orderbook = OrderBook(symbol="BTCUSD")

        # Initial snapshot
        snapshot_data = {
            "symbol": "BTCUSD",
            "timestamp": 1234567890,
            "sequence_no": 100,
            "bids": [["50000.0", "1.5"], ["49999.5", "2.0"]],
            "asks": [["50000.5", "1.0"], ["50001.0", "1.5"]],
        }

        orderbook.update_from_snapshot(snapshot_data, converter)
        initial_checksum = orderbook.compute_checksum(converter)

        # Apply update that modifies a level
        update_data = {
            "symbol": "BTCUSD",
            "timestamp": 1234567891,
            "sequence_no": 101,
            "bids": [["50000.0", "2.5"]],  # Update bid size
            "asks": [],
        }

        success = orderbook.apply_update(update_data, converter)
        assert success

        # Checksum should change
        updated_checksum = orderbook.compute_checksum(converter)
        assert updated_checksum != initial_checksum

        # Verify the checksum matches expected value
        expected_string = "50000.5:1.0,50001.0:1.5|50000.0:2.5,49999.5:2.0"
        expected_checksum = zlib.crc32(expected_string.encode()) & 0xFFFFFFFF
        assert updated_checksum == expected_checksum

    def test_checksum_with_level_removal(self, converter):
        """Test checksum after removing a level."""
        orderbook = OrderBook(symbol="BTCUSD")

        # Initial snapshot with 3 levels
        snapshot_data = {
            "symbol": "BTCUSD",
            "timestamp": 1234567890,
            "sequence_no": 100,
            "bids": [["50000.0", "1.5"], ["49999.5", "2.0"], ["49999.0", "1.0"]],
            "asks": [["50000.5", "1.0"]],
        }

        orderbook.update_from_snapshot(snapshot_data, converter)

        # Remove middle bid level
        update_data = {
            "symbol": "BTCUSD",
            "timestamp": 1234567891,
            "sequence_no": 101,
            "bids": [["49999.5", "0"]],  # Size 0 = remove
            "asks": [],
        }

        orderbook.apply_update(update_data, converter)

        # Verify level was removed
        assert len(orderbook.bids) == 2
        assert len(orderbook._raw_bids) == 2

        # Checksum should reflect the removal
        expected_string = "50000.5:1.0|50000.0:1.5,49999.0:1.0"
        expected_checksum = zlib.crc32(expected_string.encode()) & 0xFFFFFFFF
        computed = orderbook.compute_checksum(converter)
        assert computed == expected_checksum

    def test_checksum_with_more_than_10_levels(self, converter):
        """Test that checksum only uses top 10 levels."""
        orderbook = OrderBook(symbol="BTCUSD")

        # Create 15 bid and ask levels
        bids = [[f"{50000.0 - i * 0.5}", f"{i + 1}"] for i in range(15)]
        asks = [[f"{50000.5 + i * 0.5}", f"{i + 1}"] for i in range(15)]

        snapshot_data = {
            "symbol": "BTCUSD",
            "timestamp": 1234567890,
            "sequence_no": 100,
            "bids": bids,
            "asks": asks,
        }

        orderbook.update_from_snapshot(snapshot_data, converter)

        # Verify we have all 15 levels
        assert len(orderbook.bids) == 15
        assert len(orderbook.asks) == 15

        # But checksum should only use top 10
        top_10_asks = asks[:10]
        top_10_bids = bids[:10]

        ask_parts = [f"{price}:{size}" for price, size in top_10_asks]
        bid_parts = [f"{price}:{size}" for price, size in top_10_bids]
        expected_string = ",".join(ask_parts) + "|" + ",".join(bid_parts)
        expected_checksum = zlib.crc32(expected_string.encode()) & 0xFFFFFFFF

        computed = orderbook.compute_checksum(converter)
        assert computed == expected_checksum

    def test_checksum_with_empty_orderbook(self, converter):
        """Test checksum computation with empty orderbook."""
        orderbook = OrderBook(symbol="BTCUSD")

        snapshot_data = {
            "symbol": "BTCUSD",
            "timestamp": 1234567890,
            "sequence_no": 100,
            "bids": [],
            "asks": [],
        }

        orderbook.update_from_snapshot(snapshot_data, converter)

        # Empty orderbook should produce checksum of "|" (empty asks | empty bids)
        expected_checksum = zlib.crc32("|".encode()) & 0xFFFFFFFF
        computed = orderbook.compute_checksum(converter)
        assert computed == expected_checksum

    def test_checksum_preserves_decimal_precision(self, converter):
        """Test that decimal precision from server is preserved."""
        orderbook = OrderBook(symbol="BTCUSD")

        # Test with various decimal precisions
        snapshot_data = {
            "symbol": "BTCUSD",
            "timestamp": 1234567890,
            "sequence_no": 100,
            "bids": [
                ["50000", "1"],  # No decimals
                ["50000.0", "1.0"],  # One decimal
                ["50000.00", "1.00"],  # Two decimals
            ],
            "asks": [
                ["50001", "2"],
                ["50001.5", "2.5"],
                ["50001.50", "2.50"],
            ],
        }

        orderbook.update_from_snapshot(snapshot_data, converter)

        # Raw values should preserve exact string format
        assert orderbook._raw_asks[0] == ("50001", "2")
        assert orderbook._raw_asks[1] == ("50001.5", "2.5")
        assert orderbook._raw_asks[2] == ("50001.50", "2.50")

        # Checksum should use these exact strings
        expected_string = (
            "50001:2,50001.5:2.5,50001.50:2.50|50000:1,50000.0:1.0,50000.00:1.00"
        )
        expected_checksum = zlib.crc32(expected_string.encode()) & 0xFFFFFFFF
        computed = orderbook.compute_checksum(converter)
        assert computed == expected_checksum

    def test_validate_checksum_method(self, converter):
        """Test the validate_checksum convenience method."""
        orderbook = OrderBook(symbol="BTCUSD")

        snapshot_data = {
            "symbol": "BTCUSD",
            "timestamp": 1234567890,
            "sequence_no": 100,
            "bids": [["50000.0", "1.5"]],
            "asks": [["50000.5", "1.0"]],
        }

        orderbook.update_from_snapshot(snapshot_data, converter)

        # Get correct checksum
        correct_checksum = orderbook.compute_checksum(converter)

        # Validate should return True for correct checksum
        assert orderbook.validate_checksum(correct_checksum, converter) is True

        # Validate should return False for incorrect checksum
        assert orderbook.validate_checksum(12345678, converter) is False
