import asyncio

import ccxt.async_support as ccxt  # noqa: E402


async def test():
    # exchange = ccxt.binanceusdm()
    exchange = ccxt.delta()

    try:
        # orderbook = await exchange.fetch_markets(
        #     {
        #         "contract_types": "perpetual_futures",
        #         "page_size": 100,
        #     }
        # )
        orderbook1 = await exchange.fetch_order_book("1000BONKUSD")
        trades = await exchange.fetch_trades("1000BONKUSD")
        print(trades)
        # orderbook2 = await exchange.fetch_order_book("1000BONKUSD")
        await exchange.close()
        return [
            orderbook1,
            #     orderbook2
        ]
    except ccxt.BaseError as e:
        print(type(e).__name__, str(e), str(e.args))
        raise e


syms = asyncio.run(test())
print(syms[0])
