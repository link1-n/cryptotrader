"""REST API client for Delta Exchange."""

import json
from typing import Any

import aiohttp

from ..client.auth import get_auth_headers, sign_request
from ..models.product import Product
from ..utils.config import Config
from ..utils.logger import logger


class RestClient:
    """Async REST client for Delta Exchange API."""

    def __init__(self):
        self.base_url = Config.get_rest_url()
        self.session: aiohttp.ClientSession | None = None
        self._rate_limit_remaining: int | None = None
        self._rate_limit_reset: int | None = None

    async def __aenter__(self):
        """Context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        await self.close()

    async def connect(self) -> None:
        """Create aiohttp session."""
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=Config.REST_TIMEOUT)
            self.session = aiohttp.ClientSession(timeout=timeout)
            logger.info(f"REST client connected to {self.base_url}")

    async def close(self) -> None:
        """Close aiohttp session."""
        if self.session and not self.session.closed:
            await self.session.close()
            logger.info("REST client closed")

    async def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        auth_required: bool = True,
    ) -> dict[str, Any]:
        """
        Make an authenticated HTTP request.

        Args:
            method: HTTP method
            path: API path
            params: Query parameters
            data: Request body data
            auth_required: Whether authentication is required

        Returns:
            Response JSON data

        Raises:
            aiohttp.ClientError: On request failure
        """
        if self.session is None:
            await self.connect()

        if self.session is None:
            return {}

        url = f"{self.base_url}{path}"
        headers = {}

        # Build query string for signature (must match exact format sent to API)
        query_string = ""
        if params:
            query_string = "?" + "&".join(f"{k}={v}" for k, v in params.items())

        # Build request body
        body_string = ""
        if data:
            body_string = json.dumps(data)

        # Sign request if authentication required
        if auth_required:
            signature, timestamp = sign_request(
                method=method.upper(),
                path=path,
                query_string=query_string,
                body=body_string,
            )
            headers.update(get_auth_headers(signature, timestamp))

        # Make request
        try:
            async with self.session.request(
                method=method,
                url=url,
                params=params,
                json=data if data else None,
                headers=headers,
            ) as response:
                logger.debug(
                    f"REST POST request ->\nmethod: {method}\nurl: {url}\nparams: {params}\ndata: {data}\nheaders: {headers}\n{'=' * 60}"
                )
                # Track rate limits
                self._rate_limit_remaining = response.headers.get(
                    "X-RateLimit-Remaining"
                )
                self._rate_limit_reset = response.headers.get("X-RateLimit-Reset")

                # Handle response - try JSON first, fall back to text
                try:
                    response_data = await response.json()
                except (aiohttp.ContentTypeError, json.JSONDecodeError):
                    # Response is not JSON (might be HTML for 404, etc.)
                    response_text = await response.text()
                    if response.status >= 400:
                        logger.error(
                            f"REST API error: {response.status} - Non-JSON response: {response_text}"
                        )
                        raise aiohttp.ClientError(
                            f"API error {response.status}: {response_text[:100]}"
                        )
                    # For successful non-JSON responses, return empty dict
                    return {}

                if response.status >= 400:
                    error_msg = response_data.get("error", {}).get(
                        "message", "Unknown error"
                    )
                    logger.error(
                        f"REST API error: {response.status} - {error_msg} - {response_data}"
                    )
                    raise aiohttp.ClientError(
                        f"API error {response.status}: {error_msg}"
                    )

                return response_data

        except aiohttp.ClientError as e:
            logger.error(f"REST request failed: {method} {url} - {e}")
            raise

    # Public endpoints

    async def get_products(
        self, contract_types: list[str] | None = None
    ) -> list[Product]:
        """
        Get list of products.

        Args:
            contract_types: Filter by contract types (e.g., ["perpetual_futures"])

        Returns:
            List of Product objects
        """
        params = {}
        if contract_types:
            params["contract_types"] = ",".join(contract_types)

        response = await self._request(
            "GET", "/v2/products", params=params, auth_required=False
        )

        products = []
        for item in response.get("result", []):
            try:
                product = Product.from_api(item)
                products.append(product)
            except (KeyError, ValueError) as e:
                logger.warning(f"Failed to parse product: {e}")

        logger.info(f"Fetched {len(products)} products")
        return products

    async def get_product(self, symbol: str) -> Product | None:
        """
        Get a single product by symbol.

        Args:
            symbol: Product symbol

        Returns:
            Product object or None if not found
        """
        response = await self._request(
            "GET", f"/v2/products/{symbol}", auth_required=False
        )

        result = response.get("result")
        if result:
            return Product.from_api(result)
        return None

    async def get_orderbook(self, symbol: str, depth: int = 20) -> dict[str, Any]:
        """
        Get L2 orderbook snapshot.

        Args:
            symbol: Product symbol
            depth: Number of levels (default 20)

        Returns:
            Orderbook data
        """
        response = await self._request(
            "GET",
            f"/v2/l2orderbook/{symbol}",
            params={"depth": depth},
            auth_required=False,
        )
        return response.get("result", {})

    async def get_trades(self, symbol: str, limit: int = 100) -> list[dict[str, Any]]:
        """
        Get recent trades.

        Args:
            symbol: Product symbol
            limit: Number of trades (default 100)

        Returns:
            List of trade data
        """
        response = await self._request(
            "GET",
            f"/v2/trades/{symbol}",
            params={"page_size": limit},
            auth_required=False,
        )
        return response.get("result", [])

    # Private endpoints (orders)

    async def place_order(
        self,
        product_id: int,
        size: int,
        side: str,
        order_type: str = "limit_order",
        limit_price: str | None = None,
        client_order_id: str | None = None,
    ) -> dict[str, Any]:
        """
        Place a new order.

        Args:
            product_id: Product ID
            size: Order size (contracts)
            side: "buy" or "sell"
            order_type: "limit_order" or "market_order"
            limit_price: Limit price (required for limit orders)
            client_order_id: Optional client order ID

        Returns:
            Order response data
        """
        payload = {
            "product_id": product_id,
            "size": size,
            "side": side,
            "order_type": order_type,
        }

        if limit_price:
            payload["limit_price"] = limit_price

        if client_order_id:
            payload["client_order_id"] = client_order_id

        response = await self._request("POST", "/v2/orders", data=payload)
        logger.info(
            f"Order placed: {side} {size} @ {limit_price} - ID: {response.get('result', {}).get('id')}"
        )
        return response.get("result", {})

    async def cancel_order(
        self, client_order_id: str, product_id: int
    ) -> dict[str, Any]:
        """
        Cancel an order.

        Args:
            order_id: Order ID to cancel
            product_id: Optional product ID (recommended to avoid validation errors)

        Returns:
            Cancellation response data
        """
        data = {"client_order_id": client_order_id, "product_id": product_id}
        response = await self._request("DELETE", "/v2/orders", data=data)
        logger.info(f"Order cancelled: {client_order_id}")
        return response.get("result", {})

    async def cancel_all_orders(self, product_id: int | None = None) -> dict[str, Any]:
        """
        Cancel all orders.

        Args:
            product_id: Optional product ID to filter by

        Returns:
            Cancellation response data
        """
        data = {}
        if product_id:
            data["product_id"] = product_id

        response = await self._request("DELETE", "/v2/orders/all", data=data)
        logger.info(f"All orders cancelled for product_id={product_id}")
        return response.get("result", {})

    async def edit_order(
        self,
        order_id: str,
        size: int | None = None,
        limit_price: str | None = None,
    ) -> dict[str, Any]:
        """
        Edit an existing order.

        Args:
            order_id: Order ID to edit
            size: New size (contracts)
            limit_price: New limit price

        Returns:
            Updated order data
        """
        payload = {}
        if size is not None:
            payload["size"] = size
        if limit_price is not None:
            payload["limit_price"] = limit_price

        response = await self._request("PUT", f"/v2/orders/{order_id}", data=payload)
        logger.info(f"Order edited: {order_id}")
        return response.get("result", {})

    async def get_open_orders(
        self, product_id: int | None = None
    ) -> list[dict[str, Any]]:
        """
        Get all open orders.

        Args:
            product_id: Optional product ID to filter by

        Returns:
            List of order data
        """
        params = {}
        if product_id:
            params["product_id"] = product_id

        response = await self._request("GET", "/v2/orders", params=params)
        return response.get("result", [])

    async def get_order(self, order_id: str) -> dict[str, Any]:
        """
        Get order details.

        Args:
            order_id: Order ID

        Returns:
            Order data
        """
        response = await self._request("GET", f"/v2/orders/{order_id}")
        return response.get("result", {})

    async def get_positions(self) -> list[dict[str, Any]]:
        """
        Get all positions.

        Returns:
            List of position data
        """
        response = await self._request("GET", "/v2/positions")
        return response.get("result", [])

    async def get_wallet_balance(self) -> dict[str, Any]:
        """
        Get wallet balance.

        Returns:
            Wallet balance data
        """
        response = await self._request("GET", "/v2/wallet/balances")
        return response.get("result", {})

    def get_rate_limit_info(self) -> tuple[int | None, int | None]:
        """
        Get current rate limit information.

        Returns:
            Tuple of (remaining, reset_timestamp)
        """
        return self._rate_limit_remaining, self._rate_limit_reset
