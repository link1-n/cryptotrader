"""WebSocket client for Delta Exchange with reconnection and heartbeat."""

import asyncio
import json
from collections.abc import Callable
from typing import Any

import aiohttp

from ..utils.config import Config
from ..utils.logger import logger
from .auth import create_websocket_auth_message, sign_websocket_auth


class WebSocketClient:
    """Async WebSocket client for Delta Exchange."""

    def __init__(self):
        self.url = Config.get_ws_url()
        self.ws: aiohttp.ClientWebSocketResponse | None = None
        self.session: aiohttp.ClientSession | None = None
        self._running = False
        self._authenticated = False
        self._reconnect_attempts = 0
        self._max_reconnect_attempts = Config.WS_MAX_RECONNECT_ATTEMPTS

        # Channel subscriptions
        self._subscriptions: set[str] = set()
        self._channel_handlers: dict[str, list[Callable]] = {}

        # Message handlers by type
        self._message_handlers: dict[str, list[Callable]] = {
            "snapshot": [],
            "update": [],
            "l2_orderbook": [],
            "all_trades": [],
            "all_trades_snapshot": [],
            "ticker": [],
            "subscriptions": [],
            "heartbeat": [],
        }

        # Heartbeat tracking
        self._last_heartbeat = 0
        self._heartbeat_task: asyncio.Task | None = None
        self._watchdog_task: asyncio.Task | None = None

        # Main tasks
        self._receive_task: asyncio.Task | None = None
        self._connect_event = asyncio.Event()

    async def connect(self, authenticate: bool = True) -> None:
        """
        Connect to WebSocket.

        Args:
            authenticate: Whether to authenticate after connecting
        """
        if self._running:
            logger.warning("WebSocket already connected")
            return

        try:
            if self.session is None or self.session.closed:
                self.session = aiohttp.ClientSession()

            logger.info(f"Connecting to WebSocket: {self.url}")
            self.ws = await self.session.ws_connect(self.url)

            self._running = True
            self._reconnect_attempts = 0
            self._connect_event.set()

            logger.info("WebSocket connected")

            # Authenticate if required
            if authenticate and Config.API_KEY and Config.API_SECRET:
                await self._authenticate()

            # Start receive loop
            self._receive_task = asyncio.create_task(self._receive_loop())

            # Start heartbeat monitoring
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            self._watchdog_task = asyncio.create_task(self._watchdog_loop())

            # Resubscribe to channels
            await self._resubscribe_all()

        except Exception as e:
            logger.error(f"WebSocket connection failed: {e}")
            self._running = False
            await self._schedule_reconnect()

    async def disconnect(self) -> None:
        """Disconnect from WebSocket."""
        logger.info("Disconnecting WebSocket")
        self._running = False
        self._connect_event.clear()

        # Cancel tasks
        if self._receive_task:
            self._receive_task.cancel()
            try:
                await self._receive_task
            except asyncio.CancelledError:
                pass
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
        if self._watchdog_task:
            self._watchdog_task.cancel()
            try:
                await self._watchdog_task
            except asyncio.CancelledError:
                pass

        # Close WebSocket
        if self.ws and not self.ws.closed:
            await self.ws.close()

        # Close session properly
        if self.session and not self.session.closed:
            await self.session.close()
            # Give it a moment to clean up
            await asyncio.sleep(0.1)

        logger.info("WebSocket disconnected")

    async def _authenticate(self) -> None:
        """Authenticate WebSocket connection."""
        try:
            signature, timestamp = sign_websocket_auth()
            auth_message = create_websocket_auth_message(signature, timestamp)

            await self._send_message(auth_message)
            self._authenticated = True
            logger.info("WebSocket authenticated")

        except Exception as e:
            logger.error(f"WebSocket authentication failed: {e}")
            self._authenticated = False

    async def _send_message(self, message: dict[str, Any]) -> None:
        """Send a message to WebSocket."""
        if self.ws and not self.ws.closed:
            await self.ws.send_json(message)
        else:
            logger.warning("Cannot send message: WebSocket not connected")

    async def _receive_loop(self) -> None:
        """Main receive loop for WebSocket messages."""
        try:
            if not self.ws:
                raise AssertionError("NO WS")
            async for msg in self.ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        data = json.loads(msg.data)
                        await self._handle_message(data)
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to decode message: {e}")
                    except Exception as e:
                        logger.error(f"Error handling message: {e}")

                elif msg.type == aiohttp.WSMsgType.ERROR:
                    logger.error(f"WebSocket error: {self.ws.exception()}")
                    break

                elif msg.type == aiohttp.WSMsgType.CLOSED:
                    logger.warning("WebSocket closed by server")
                    break

        except asyncio.CancelledError:
            logger.debug("Receive loop cancelled")
        except Exception as e:
            logger.error(f"Receive loop error: {e}")
        finally:
            if self._running:
                await self._schedule_reconnect()

    async def _handle_message(self, data: dict[str, Any]) -> None:
        """Handle incoming WebSocket message."""
        # logger.debug(f"RECV WS MSG -> {data}")
        msg_type = data.get("type")
        # logger.debug(f"MSG TYPE: {msg_type}")

        # Handle heartbeat
        if msg_type == "heartbeat":
            self._last_heartbeat = asyncio.get_event_loop().time()
            await self._send_message({"type": "heartbeat"})
            return

        # Handle subscriptions confirmation
        if msg_type == "subscriptions":
            logger.debug(f"Subscriptions confirmed: {data.get('channels', [])}")
            for handler in self._message_handlers.get("subscriptions", []):
                asyncio.create_task(handler(data))
            return

        # Handle l2_orderbook (full snapshot from Delta Exchange)
        if msg_type == "l2_orderbook":
            symbol = data.get("symbol")
            if symbol:
                channel_key = f"l2_orderbook.{symbol}"
                if channel_key in self._channel_handlers:
                    for handler in self._channel_handlers[channel_key]:
                        asyncio.create_task(handler(data))

            # Also call generic handlers
            for handler in self._message_handlers.get("l2_orderbook", []):
                asyncio.create_task(handler(data))
            return

        # Handle l2_updates (snapshot and update) - for incremental updates
        if msg_type in ["snapshot", "update"]:
            symbol = data.get("symbol")
            if symbol:
                channel_key = f"l2_orderbook.{symbol}"
                if channel_key in self._channel_handlers:
                    for handler in self._channel_handlers[channel_key]:
                        asyncio.create_task(handler(data))

            # Also call generic handlers
            for handler in self._message_handlers.get(msg_type, []):
                asyncio.create_task(handler(data))
            return

        # Handle all_trades_snapshot (initial snapshot of trades)
        if msg_type == "all_trades_snapshot":
            symbol = data.get("symbol")
            if symbol:
                channel_key = f"all_trades.{symbol}"
                if channel_key in self._channel_handlers:
                    for handler in self._channel_handlers[channel_key]:
                        asyncio.create_task(handler(data))

            for handler in self._message_handlers.get("all_trades_snapshot", []):
                asyncio.create_task(handler(data))
            return

        # Handle all_trades (live trade updates)
        if msg_type == "all_trades":
            symbol = data.get("symbol")
            if symbol:
                channel_key = f"all_trades.{symbol}"
                if channel_key in self._channel_handlers:
                    for handler in self._channel_handlers[channel_key]:
                        asyncio.create_task(handler(data))

            for handler in self._message_handlers.get("all_trades", []):
                asyncio.create_task(handler(data))
            return

        # Handle ticker
        if msg_type == "v2/ticker":
            symbol = data.get("symbol")
            if symbol:
                channel_key = f"v2/ticker.{symbol}"
                if channel_key in self._channel_handlers:
                    for handler in self._channel_handlers[channel_key]:
                        asyncio.create_task(handler(data))

            for handler in self._message_handlers.get("ticker", []):
                asyncio.create_task(handler(data))
            return

        # Generic message type handlers
        if msg_type in self._message_handlers:
            for handler in self._message_handlers[msg_type]:
                asyncio.create_task(handler(data))

    async def subscribe(self, channels: list[str]) -> None:
        """
        Subscribe to channels.

        Args:
            channels: List of channel.symbol pairs (e.g., ["l2_orderbook.BTCUSD", "all_trades.BTCUSD"])
        """
        if not self._running:
            logger.warning(
                "WebSocket not connected, channels will be subscribed on connect"
            )

        # Add to subscription set
        self._subscriptions.update(channels)

        # Send subscription message if connected
        if self.ws and not self.ws.closed:
            # Parse channels into proper format for Delta Exchange
            # Convert ["l2_orderbook.BTCUSD", "all_trades.BTCUSD"] to
            # [{"name": "l2_orderbook", "symbols": ["BTCUSD"]}, ...]
            channel_map = {}
            for channel_str in channels:
                parts = channel_str.split(".", 1)
                if len(parts) == 2:
                    channel_name, symbol = parts
                    if channel_name not in channel_map:
                        channel_map[channel_name] = []
                    channel_map[channel_name].append(symbol)

            # Build subscription payload in Delta Exchange format
            channel_list = [
                {"name": name, "symbols": symbols}
                for name, symbols in channel_map.items()
            ]

            sub_message = {"type": "subscribe", "payload": {"channels": channel_list}}
            logger.debug(f"channel message: {sub_message}")
            await self._send_message(sub_message)
            logger.info(f"Subscribed to channels: {channels}")

    async def unsubscribe(self, channels: list[str]) -> None:
        """
        Unsubscribe from channels.

        Args:
            channels: List of channel names to unsubscribe
        """
        # Remove from subscription set
        for channel in channels:
            self._subscriptions.discard(channel)

        # Send unsubscribe message if connected
        if self.ws and not self.ws.closed:
            # Parse channels into proper format
            channel_map = {}
            for channel_str in channels:
                parts = channel_str.split(".", 1)
                if len(parts) == 2:
                    channel_name, symbol = parts
                    if channel_name not in channel_map:
                        channel_map[channel_name] = []
                    channel_map[channel_name].append(symbol)

            channel_list = [
                {"name": name, "symbols": symbols}
                for name, symbols in channel_map.items()
            ]

            unsub_message = {
                "type": "unsubscribe",
                "payload": {"channels": channel_list},
            }
            await self._send_message(unsub_message)
            logger.info(f"Unsubscribed from channels: {channels}")

    async def _resubscribe_all(self) -> None:
        """Resubscribe to all channels after reconnection."""
        if self._subscriptions:
            channels = list(self._subscriptions)
            logger.info(f"Resubscribing to {len(channels)} channels")
            await self.subscribe(channels)

    def add_handler(self, channel_or_type: str, handler: Callable) -> None:
        """
        Add a message handler for a channel or message type.

        Args:
            channel_or_type: Channel name (e.g., "l2_orderbook.BTCUSD") or message type (e.g., "snapshot")
            handler: Async callback function
        """
        # Check if it's a specific channel
        if "." in channel_or_type or channel_or_type.startswith("v2/"):
            if channel_or_type not in self._channel_handlers:
                self._channel_handlers[channel_or_type] = []
            self._channel_handlers[channel_or_type].append(handler)
        else:
            # It's a message type
            if channel_or_type not in self._message_handlers:
                self._message_handlers[channel_or_type] = []
            self._message_handlers[channel_or_type].append(handler)

    def remove_handler(self, channel_or_type: str, handler: Callable) -> None:
        """
        Remove a message handler.

        Args:
            channel_or_type: Channel name or message type
            handler: Handler to remove
        """
        if "." in channel_or_type or channel_or_type.startswith("v2/"):
            if channel_or_type in self._channel_handlers:
                self._channel_handlers[channel_or_type].remove(handler)
        else:
            if channel_or_type in self._message_handlers:
                self._message_handlers[channel_or_type].remove(handler)

    async def _heartbeat_loop(self) -> None:
        """Send periodic heartbeat to keep connection alive."""
        try:
            while self._running:
                await asyncio.sleep(Config.WS_HEARTBEAT_INTERVAL)
                if self.ws and not self.ws.closed:
                    await self._send_message({"type": "enable_heartbeat"})
        except asyncio.CancelledError:
            logger.debug("Heartbeat loop cancelled")

    async def _watchdog_loop(self) -> None:
        """Monitor connection health and reconnect if needed."""
        try:
            while self._running:
                await asyncio.sleep(60)  # Check every 60 seconds

                # Check if we received a heartbeat recently
                current_time = asyncio.get_event_loop().time()
                if self._last_heartbeat > 0:
                    time_since_heartbeat = current_time - self._last_heartbeat
                    if time_since_heartbeat > 120:  # No heartbeat for 2 minutes
                        logger.warning(
                            f"No heartbeat received for {time_since_heartbeat:.0f}s, reconnecting"
                        )
                        await self._schedule_reconnect()
                        break

                # Check if WebSocket is still open
                if self.ws and self.ws.closed:
                    logger.warning("WebSocket closed unexpectedly, reconnecting")
                    await self._schedule_reconnect()
                    break

        except asyncio.CancelledError:
            logger.debug("Watchdog loop cancelled")

    async def _schedule_reconnect(self) -> None:
        """Schedule a reconnection attempt."""
        if not self._running:
            return

        if self._reconnect_attempts >= self._max_reconnect_attempts:
            logger.error(
                f"Max reconnection attempts ({self._max_reconnect_attempts}) reached"
            )
            self._running = False
            return

        self._reconnect_attempts += 1
        delay = Config.WS_RECONNECT_DELAY * self._reconnect_attempts

        logger.info(
            f"Reconnecting in {delay}s (attempt {self._reconnect_attempts}/{self._max_reconnect_attempts})"
        )

        await asyncio.sleep(delay)

        # Check if still running and not already connected
        if self._running and (not self.ws or self.ws.closed):
            await self.connect(authenticate=True)

    async def wait_connected(self, timeout: float = 10.0) -> bool:
        """
        Wait for WebSocket to be connected.

        Args:
            timeout: Maximum time to wait in seconds

        Returns:
            True if connected, False if timeout
        """
        try:
            await asyncio.wait_for(self._connect_event.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False

    @property
    def is_connected(self) -> bool:
        """Check if WebSocket is connected."""
        return self._running and self.ws is not None and not self.ws.closed

    @property
    def is_authenticated(self) -> bool:
        """Check if WebSocket is authenticated."""
        return self._authenticated
