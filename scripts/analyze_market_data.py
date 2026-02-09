"""Utility script to analyze recorded market data from Parquet files."""

import argparse
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)
pd.set_option("display.max_rows", 100)


def load_orderbook_data(
    symbol: str, data_dir: str = "market_data", date: str | None = None
) -> pd.DataFrame:
    """
    Load orderbook data for a symbol.

    Args:
        symbol: Trading symbol (e.g., "XRPUSD")
        data_dir: Directory containing Parquet files
        date: Date string in YYYYMMDD format (default: today)

    Returns:
        DataFrame with orderbook data
    """
    data_path = Path(data_dir)

    if date is None:
        date = datetime.utcnow().strftime("%Y%m%d")

    filename = data_path / f"orderbook_{symbol}_{date}.parquet"

    if not filename.exists():
        print(f"File not found: {filename}")
        return pd.DataFrame()

    df = pd.read_parquet(filename)
    print(f"Loaded {len(df)} orderbook records from {filename.name}")
    return df


def load_trade_data(
    symbol: str, data_dir: str = "market_data", date: str | None = None
) -> pd.DataFrame:
    """
    Load trade data for a symbol.

    Args:
        symbol: Trading symbol (e.g., "XRPUSD")
        data_dir: Directory containing Parquet files
        date: Date string in YYYYMMDD format (default: today)

    Returns:
        DataFrame with trade data
    """
    data_path = Path(data_dir)

    if date is None:
        date = datetime.utcnow().strftime("%Y%m%d")

    filename = data_path / f"trades_{symbol}_{date}.parquet"

    if not filename.exists():
        print(f"File not found: {filename}")
        return pd.DataFrame()

    df = pd.read_parquet(filename)
    print(f"Loaded {len(df)} trade records from {filename.name}")
    return df


def load_date_range(
    symbol: str,
    data_type: str,
    data_dir: str = "market_data",
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """
    Load data across multiple dates.

    Args:
        symbol: Trading symbol
        data_type: "orderbook" or "trades"
        data_dir: Directory containing Parquet files
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format

    Returns:
        Combined DataFrame
    """
    data_path = Path(data_dir)

    if start_date is None:
        start_date = datetime.utcnow().strftime("%Y%m%d")
    if end_date is None:
        end_date = start_date

    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")

    dfs = []
    current = start

    while current <= end:
        date_str = current.strftime("%Y%m%d")
        filename = data_path / f"{data_type}_{symbol}_{date_str}.parquet"

        if filename.exists():
            df = pd.read_parquet(filename)
            dfs.append(df)
            print(f"Loaded {len(df)} records from {filename.name}")

        current += timedelta(days=1)

    if not dfs:
        print(f"No data found for {symbol} between {start_date} and {end_date}")
        return pd.DataFrame()

    combined = pd.concat(dfs, ignore_index=True)
    print(f"\nTotal records: {len(combined)}")
    return combined


def analyze_orderbook(df: pd.DataFrame) -> None:
    """Print orderbook analysis."""
    if df.empty:
        print("No data to analyze")
        return

    print("\n" + "=" * 80)
    print("ORDERBOOK ANALYSIS")
    print("=" * 80)

    # Time range
    print(f"\nTime Range:")
    print(f"  Start: {df['timestamp'].min()}")
    print(f"  End:   {df['timestamp'].max()}")
    print(f"  Duration: {df['timestamp'].max() - df['timestamp'].min()}")

    # Basic stats
    print(f"\nBasic Statistics:")
    print(f"  Total records: {len(df)}")
    print(f"  Symbols: {df['symbol'].unique().tolist()}")

    # Spread analysis
    if "spread" in df.columns:
        print(f"\nSpread Analysis (integer units):")
        print(f"  Mean: {df['spread'].mean():.2f}")
        print(f"  Median: {df['spread'].median():.2f}")
        print(f"  Min: {df['spread'].min()}")
        print(f"  Max: {df['spread'].max()}")
        print(f"  Std: {df['spread'].std():.2f}")

    # Best bid/ask
    if "best_bid_price" in df.columns and "best_ask_price" in df.columns:
        print(f"\nBest Bid:")
        print(f"  Min: {df['best_bid_price'].min()}")
        print(f"  Max: {df['best_bid_price'].max()}")
        print(f"  Mean: {df['best_bid_price'].mean():.2f}")

        print(f"\nBest Ask:")
        print(f"  Min: {df['best_ask_price'].min()}")
        print(f"  Max: {df['best_ask_price'].max()}")
        print(f"  Mean: {df['best_ask_price'].mean():.2f}")

    # Volume
    if "total_bid_volume" in df.columns and "total_ask_volume" in df.columns:
        print(f"\nVolume (top 20 levels):")
        print(f"  Avg Bid Volume: {df['total_bid_volume'].mean():.2f}")
        print(f"  Avg Ask Volume: {df['total_ask_volume'].mean():.2f}")

    # Depth
    if "num_bid_levels" in df.columns and "num_ask_levels" in df.columns:
        print(f"\nBook Depth:")
        print(f"  Avg Bid Levels: {df['num_bid_levels'].mean():.2f}")
        print(f"  Avg Ask Levels: {df['num_ask_levels'].mean():.2f}")

    # Recent records
    print(f"\nMost Recent Records:")
    print(
        df.sort_values("timestamp", ascending=False)[
            [
                "timestamp",
                "symbol",
                "best_bid_price",
                "best_bid_size",
                "best_ask_price",
                "best_ask_size",
                "spread",
            ]
        ].head(10)
    )


def analyze_trades(df: pd.DataFrame) -> None:
    """Print trade analysis."""
    if df.empty:
        print("No data to analyze")
        return

    print("\n" + "=" * 80)
    print("TRADE ANALYSIS")
    print("=" * 80)

    # Time range
    print(f"\nTime Range:")
    print(f"  Start: {df['timestamp'].min()}")
    print(f"  End:   {df['timestamp'].max()}")
    print(f"  Duration: {df['timestamp'].max() - df['timestamp'].min()}")

    # Basic stats
    print(f"\nBasic Statistics:")
    print(f"  Total trades: {len(df)}")
    print(f"  Symbols: {df['symbol'].unique().tolist()}")

    # Side distribution
    if "side" in df.columns:
        side_counts = df["side"].value_counts()
        print(f"\nSide Distribution:")
        for side, count in side_counts.items():
            pct = (count / len(df)) * 100
            print(f"  {side}: {count} ({pct:.1f}%)")

    # Price stats
    if "price" in df.columns:
        print(f"\nPrice (integer units):")
        print(f"  Min: {df['price'].min()}")
        print(f"  Max: {df['price'].max()}")
        print(f"  Mean: {df['price'].mean():.2f}")
        print(f"  Median: {df['price'].median():.2f}")

    # Size stats
    if "size" in df.columns:
        print(f"\nSize:")
        print(f"  Total: {df['size'].sum()}")
        print(f"  Mean: {df['size'].mean():.2f}")
        print(f"  Median: {df['size'].median():.2f}")
        print(f"  Min: {df['size'].min()}")
        print(f"  Max: {df['size'].max()}")

    # Recent trades
    print(f"\nMost Recent Trades:")
    print(
        df.sort_values("timestamp", ascending=False)[
            ["timestamp", "symbol", "side", "price", "size", "trade_id"]
        ].head(20)
    )

    # Trade frequency
    if len(df) > 1:
        df_sorted = df.sort_values("timestamp")
        time_diffs = df_sorted["timestamp"].diff().dt.total_seconds()
        print(f"\nTrade Frequency:")
        print(f"  Avg time between trades: {time_diffs.mean():.2f}s")
        print(f"  Min time between trades: {time_diffs.min():.2f}s")
        print(f"  Max time between trades: {time_diffs.max():.2f}s")


def list_available_files(data_dir: str = "market_data") -> None:
    """List all available Parquet files."""
    data_path = Path(data_dir)

    if not data_path.exists():
        print(f"Directory not found: {data_dir}")
        return

    files = sorted(data_path.glob("*.parquet"))

    if not files:
        print(f"No Parquet files found in {data_dir}")
        return

    print(f"\nAvailable files in {data_dir}:")
    print("-" * 80)

    for file in files:
        size_mb = file.stat().st_size / (1024 * 1024)
        try:
            parquet_file = pq.ParquetFile(file)
            num_rows = parquet_file.metadata.num_rows
            print(f"  {file.name:50s} - {size_mb:6.2f} MB - {num_rows:,} rows")
        except Exception as e:
            print(f"  {file.name:50s} - {size_mb:6.2f} MB - Error reading: {e}")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Analyze recorded market data from Parquet files"
    )
    parser.add_argument(
        "command",
        choices=["list", "orderbook", "trades", "both"],
        help="Command to execute",
    )
    parser.add_argument("-s", "--symbol", default="XRPUSD", help="Trading symbol")
    parser.add_argument(
        "-d", "--data-dir", default="market_data", help="Data directory"
    )
    parser.add_argument(
        "--date", help="Date in YYYYMMDD format (default: today)", default=None
    )
    parser.add_argument(
        "--start-date", help="Start date for range query (YYYYMMDD)", default=None
    )
    parser.add_argument(
        "--end-date", help="End date for range query (YYYYMMDD)", default=None
    )
    parser.add_argument(
        "--export", help="Export to CSV file", default=None, metavar="FILENAME"
    )

    args = parser.parse_args()

    if args.command == "list":
        list_available_files(args.data_dir)
        return

    # Load data
    if args.start_date or args.end_date:
        # Date range query
        if args.command == "orderbook":
            df = load_date_range(
                args.symbol, "orderbook", args.data_dir, args.start_date, args.end_date
            )
            analyze_orderbook(df)
        elif args.command == "trades":
            df = load_date_range(
                args.symbol, "trades", args.data_dir, args.start_date, args.end_date
            )
            analyze_trades(df)
        elif args.command == "both":
            ob_df = load_date_range(
                args.symbol, "orderbook", args.data_dir, args.start_date, args.end_date
            )
            analyze_orderbook(ob_df)

            trade_df = load_date_range(
                args.symbol, "trades", args.data_dir, args.start_date, args.end_date
            )
            analyze_trades(trade_df)
    else:
        # Single date query
        if args.command == "orderbook":
            df = load_orderbook_data(args.symbol, args.data_dir, args.date)
            analyze_orderbook(df)
        elif args.command == "trades":
            df = load_trade_data(args.symbol, args.data_dir, args.date)
            analyze_trades(df)
        elif args.command == "both":
            ob_df = load_orderbook_data(args.symbol, args.data_dir, args.date)
            analyze_orderbook(ob_df)

            trade_df = load_trade_data(args.symbol, args.data_dir, args.date)
            analyze_trades(trade_df)

        # Export if requested
        if args.export and "df" in locals():
            df.to_csv(args.export, index=False)
            print(f"\nData exported to {args.export}")


if __name__ == "__main__":
    main()
