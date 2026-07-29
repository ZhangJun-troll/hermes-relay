#!/usr/bin/env python3
"""Hermes Agent Bridge — connects local PTY to relay server.

Usage:
    python bridge.py --server ws://your-server:8080 --secret <agent-secret>
    python bridge.py --server wss://chat.xn--e1tn50j.xyz --secret <agent-secret>
    python bridge.py --server ws://localhost:8080 --secret key --name "我的Agent"
"""
import argparse
import asyncio
import json
import logging
import re
import secrets as secrets_mod

import aiohttp
from aiohttp import WSMsgType
import ssl

log = logging.getLogger("hermes-bridge")

DASHBOARD_URL = "http://127.0.0.1:9119"


async def fetch_token() -> str:
    """Get Hermes dashboard session token."""
    import os
    env_token = os.environ.get("HERMES_DASHBOARD_SESSION_TOKEN", "")
    if env_token:
        return env_token
    try:
        async with aiohttp.ClientSession() as sess:
            async with sess.get(DASHBOARD_URL) as resp:
                html = await resp.text()
                m = re.search(r'__HERMES_SESSION_TOKEN__\s*=\s*["\']([^"\']+)["\']', html)
                if m:
                    return m.group(1)
    except Exception as e:
        log.warning("Token fetch failed: %s", e)
    return ""


async def bridge_loop(server_url: str, agent_secret: str, name: str):
    """Connect to relay server and bridge to local PTY."""
    token = await fetch_token()
    if not token:
        log.error("Cannot get dashboard token. Is Hermes running?")
        return

    ws_url = f"{server_url}/ws/agent?secret={agent_secret}&name={name}"
    log.info("Connecting to relay: %s", server_url)

    # ponytail: self-signed cert trust — native app pins cert, bridge just skips verify
    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE if server_url.startswith("wss") else ssl.CERT_REQUIRED

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                relay_ws = await session.ws_connect(ws_url, heartbeat=30, ssl=ssl_ctx if server_url.startswith("wss") else None)
                log.info("Connected to relay server")

                # Wait for registration message
                msg = await relay_ws.receive()
                if msg.type == WSMsgType.TEXT:
                    reg = json.loads(msg.data)
                    log.info("Registered as agent: %s (id: %s)", reg.get("name"), reg.get("id"))

                # Handle messages from relay
                pty_ws = None
                pty_session = None

                async def connect_pty(fresh=False):
                    nonlocal pty_ws, pty_session
                    if pty_ws:
                        try: await pty_ws.close()
                        except: pass
                    if pty_session:
                        try: await pty_session.close()
                        except: pass

                    pty_session = aiohttp.ClientSession()
                    params = {
                        "token": token,
                        "channel": f"bridge-{secrets_mod.token_hex(8)}",
                        "attach": secrets_mod.token_hex(16),
                    }
                    if fresh:
                        params["fresh"] = "1"
                    pty_url = f"{DASHBOARD_URL.replace('http','ws')}/api/pty?" + "&".join(f"{k}={v}" for k,v in params.items())
                    pty_ws = await pty_session.ws_connect(pty_url)
                    log.info("PTY connected (fresh=%s)", fresh)
                    return pty_ws

                async def relay_pty_to_server():
                    """Forward PTY output to relay server."""
                    while True:
                        if not pty_ws:
                            await asyncio.sleep(0.1)
                            continue
                        try:
                            msg = await pty_ws.receive()
                            if msg.type == WSMsgType.TEXT:
                                await relay_ws.send_str(json.dumps({"type": "data", "data": msg.data}))
                            elif msg.type == WSMsgType.BINARY:
                                await relay_ws.send_bytes(msg.data)
                            elif msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.CLOSED):
                                break
                        except Exception as e:
                            log.error("PTY read error: %s", e)
                            break

                async def handle_relay_messages():
                    """Handle messages from relay server."""
                    nonlocal pty_ws
                    async for msg in relay_ws:
                        if msg.type == WSMsgType.TEXT:
                            try:
                                d = json.loads(msg.data)
                                if d.get("type") == "client_connect":
                                    fresh = d.get("fresh", False)
                                    await connect_pty(fresh=fresh)
                                    # Start PTY relay
                                    asyncio.create_task(relay_pty_to_server())
                                elif d.get("type") == "client_disconnect":
                                    if pty_ws:
                                        try: await pty_ws.close()
                                        except: pass
                                        pty_ws = None
                                    log.info("Client disconnected, PTY closed")
                                elif d.get("type") == "data":
                                    if pty_ws:
                                        await pty_ws.send_str(d["data"])
                                elif d.get("type") == "resize":
                                    if pty_ws:
                                        cols = d.get("cols", 80)
                                        rows = d.get("rows", 24)
                                        await pty_ws.send_str(f"\x1b[RESIZE:{cols};{rows}]")
                            except json.JSONDecodeError:
                                pass
                        elif msg.type == WSMsgType.BINARY:
                            if pty_ws:
                                await pty_ws.send_bytes(msg.data)
                        elif msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.CLOSED):
                            break

                await handle_relay_messages()

            except Exception as e:
                log.error("Connection lost: %s", e)
            log.info("Reconnecting in 5s...")
            await asyncio.sleep(5)


def main():
    parser = argparse.ArgumentParser(description="Hermes Agent Bridge")
    parser.add_argument("--server", required=True, help="Relay server URL (ws://host:port)")
    parser.add_argument("--secret", required=True, help="Agent registration secret")
    parser.add_argument("--name", default="unnamed", help="Agent display name")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    asyncio.run(bridge_loop(args.server.rstrip("/"), args.secret, args.name))


if __name__ == "__main__":
    main()
