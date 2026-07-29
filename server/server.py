#!/usr/bin/env python3
"""Hermes Relay Server — agents connect in, clients connect out.

Usage:
    python server.py                      # default 0.0.0.0:8080
    python server.py --port 9000          # custom port
    python server.py --password mypass    # client access password
    python server.py --agent-secret key   # agent registration secret

Architecture:
    Agent (user's PC) --ws--> Server <--ws-- Browser Client
    Server routes messages between them.
"""
import argparse
import asyncio
import json
import logging
import secrets
import time

import aiohttp
from aiohttp import web, WSMsgType

log = logging.getLogger("relay-server")

# ── Agent registry ───────────────────────────────────────────────────────
# Each agent has: ws, name, connected_at, queue (messages from agent)
agents: dict[str, dict] = {}


# ── HTML ─────────────────────────────────────────────────────────────────

HTML = r"""<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">
<title>Hermes Relay</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@xterm/xterm@5.5.0/css/xterm.css">
<style>
*{margin:0;padding:0;box-sizing:border-box}
html,body{height:100%;background:#0a0a0a;overflow:hidden;font-family:-apple-system,BlinkMacSystemFont,sans-serif}
#login{display:flex;align-items:center;justify-content:center;height:100vh;background:#0a0a0a}
#login form{background:#1a1a1a;padding:2rem;border-radius:12px;width:min(90vw,360px)}
#login h2{color:#f0e6d2;margin-bottom:1rem;font-size:1.2rem;text-align:center}
#login input{width:100%;padding:.75rem;margin-bottom:1rem;border:1px solid #333;border-radius:8px;background:#0a0a0a;color:#f0e6d2;font-size:1rem;outline:none}
#login input:focus{border-color:#0ea5e9}
#login button{width:100%;padding:.75rem;border:none;border-radius:8px;background:#0ea5e9;color:#fff;font-size:1rem;cursor:pointer}
#login button:hover{background:#0284c7}
#login .err{color:#ef4444;font-size:.85rem;margin-bottom:.5rem;display:none}
#login .hint{color:#666;font-size:.8rem;text-align:center;margin-top:.5rem}
#agent-list{margin-bottom:1rem}
#agent-list select{width:100%;padding:.75rem;border:1px solid #333;border-radius:8px;background:#0a0a0a;color:#f0e6d2;font-size:1rem}
#term-wrap{display:none;height:100vh;width:100vw}
#term{height:100%;width:100%}
#status{position:fixed;top:8px;right:12px;padding:4px 10px;border-radius:12px;font-size:.75rem;z-index:10;opacity:.9;transition:opacity .3s}
.connected{background:#22c55e22;color:#22c55e}
.connecting{background:#eab30822;color:#eab308}
.disconnected{background:#ef444422;color:#ef4444}
#toolbar{position:fixed;bottom:8px;right:8px;display:flex;gap:6px;z-index:10}
#toolbar button{background:#1a1a1acc;border:1px solid #333;color:#999;padding:6px 10px;border-radius:6px;font-size:.75rem;cursor:pointer;backdrop-filter:blur(8px)}
#toolbar button:hover{color:#f0e6d2;border-color:#553}
</style>
</head>
<body>
<div id="login">
  <form onsubmit="doLogin(event)">
    <h2>🤖 Hermes Relay</h2>
    <div class="err" id="login-err"></div>
    <div id="agent-list"><select id="agent-sel"><option value="">加载中...</option></select></div>
    <input id="pw" type="password" placeholder="输入密码" autofocus autocomplete="current-password">
    <button type="submit">连接</button>
    <div class="hint">选择一个 Agent，输入密码连接</div>
  </form>
</div>
<div id="term-wrap">
  <div id="status" class="connecting">连接中...</div>
  <div id="toolbar">
    <button onclick="doReconnect()">↻ 重连</button>
    <button onclick="doNewSession()">+ 新会话</button>
    <button onclick="doLogout()">退出</button>
  </div>
  <div id="term"></div>
</div>
<script src="https://cdn.jsdelivr.net/npm/@xterm/xterm@5.5.0/lib/xterm.js"></script>
<script src="https://cdn.jsdelivr.net/npm/@xterm/addon-fit@0.10.0/lib/addon-fit.js"></script>
<script src="https://cdn.jsdelivr.net/npm/@xterm/addon-web-links@0.11.0/lib/addon-web-links.js"></script>
<script>
const FitAddon=window.FitAddon?.FitAddon;
const WebLinksAddon=window.WebLinksAddon?.WebLinksAddon;
let term,ws,fitAddon,password='';
const statusEl=document.getElementById('status');
const loginEl=document.getElementById('login');
const termWrap=document.getElementById('term-wrap');
const loginErr=document.getElementById('login-err');

function setStatus(cls,text){statusEl.className=cls;statusEl.textContent=text;statusEl.style.opacity='1';clearTimeout(statusEl._timer);if(cls==='connected')statusEl._timer=setTimeout(()=>statusEl.style.opacity='0',3000)}

async function loadAgents(){
  try{
    const r=await fetch('/agents');
    const d=await r.json();
    const sel=document.getElementById('agent-sel');
    sel.innerHTML='';
    if(!d.length){sel.innerHTML='<option value="">无可用 Agent</option>';return}
    d.forEach(a=>{const o=document.createElement('option');o.value=a.id;o.textContent=a.name+' ('+a.id.slice(0,8)+')';sel.appendChild(o)})
  }catch(e){document.getElementById('agent-sel').innerHTML='<option value="">加载失败</option>'}
}
loadAgents();

function doLogin(e){
  e.preventDefault();
  const agentId=document.getElementById('agent-sel').value;
  if(!agentId){loginErr.textContent='请选择一个 Agent';loginErr.style.display='block';return}
  password=document.getElementById('pw').value;
  if(!password)return;
  loginErr.style.display='none';
  connectWS(agentId,password,false);
}

function connectWS(agentId,pw,fresh){
  if(ws){try{ws.close()}catch(_){}}
  setStatus('connecting','连接中...');
  const proto=location.protocol==='https:'?'wss:':'ws:';
  let url=proto+'//'+location.host+'/ws/client?agent='+encodeURIComponent(agentId)+'&password='+encodeURIComponent(pw);
  if(fresh)url+='&fresh=1';
  ws=new WebSocket(url);
  ws.binaryType='arraybuffer';
  ws.onopen=()=>{
    loginEl.style.display='none';termWrap.style.display='block';
    setStatus('connected','已连接');
    if(!term)initTerm();
    setTimeout(()=>{if(fitAddon)fitAddon.fit()},100);
  };
  ws.onmessage=(e)=>{
    if(term){
      if(e.data instanceof ArrayBuffer)term.write(new Uint8Array(e.data));
      else term.write(e.data);
    }
  };
  ws.onclose=(e)=>{
    if(e.code===4001){loginErr.textContent='密码错误';loginErr.style.display='block';loginEl.style.display='flex';termWrap.style.display='none';return}
    if(e.code===4004){loginErr.textContent='Agent 不在线';loginErr.style.display='block';loginEl.style.display='flex';termWrap.style.display='none';return}
    setStatus('disconnected','已断开 ('+e.code+')');
  };
  ws.onerror=()=>setStatus('disconnected','连接错误');
}

function initTerm(){
  term=new Terminal({
    cursorBlink:true,fontSize:getFontSize(),
    fontFamily:"'JetBrains Mono','Cascadia Mono','Fira Code',Menlo,Consolas,monospace",
    lineHeight:1.15,
    theme:{background:'#0a0a0a',foreground:'#f0e6d2',cursor:'#f0e6d2',selectionBackground:'#f0e6d244'},
    allowProposedApi:true,macOptionIsMeta:true,
  });
  fitAddon=new FitAddon();term.loadAddon(fitAddon);
  if(WebLinksAddon)term.loadAddon(new WebLinksAddon());
  term.open(document.getElementById('term'));fitAddon.fit();
  term.onData(data=>{if(ws&&ws.readyState===WebSocket.OPEN)ws.send(data)});
  term.onResize(({cols,rows})=>{if(ws&&ws.readyState===WebSocket.OPEN)ws.send('\x1b[RESIZE:'+cols+';'+rows+']')});
  new ResizeObserver(()=>{if(fitAddon)fitAddon.fit()}).observe(document.getElementById('term'));
}
function getFontSize(){return window.innerWidth<400?11:window.innerWidth<700?13:15}
function doReconnect(){const a=document.getElementById('agent-sel').value;if(a)connectWS(a,password,false)}
function doNewSession(){const a=document.getElementById('agent-sel').value;if(a)connectWS(a,password,true)}
function doLogout(){if(ws)ws.close();loginEl.style.display='flex';termWrap.style.display='none';loadAgents()}
</script>
</body>
</html>"""


# ── Handlers ─────────────────────────────────────────────────────────────

async def handle_index(request: web.Request) -> web.Response:
    return web.Response(text=HTML, content_type="text/html")


async def handle_agents(request: web.Request) -> web.Response:
    """List connected agents."""
    now = time.time()
    result = [
        {"id": aid, "name": a["name"], "uptime": int(now - a["connected_at"])}
        for aid, a in agents.items()
    ]
    return web.json_response(result)


async def handle_agent_ws(request: web.Request) -> web.WebSocketResponse:
    """Agent registers itself here. We read from agent and queue messages."""
    secret = request.query.get("secret", "")
    if secret != request.app["agent_secret"]:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.close(code=4001, reason="bad secret")
        return ws

    name = request.query.get("name", "unnamed")
    agent_id = secrets.token_hex(8)
    queue: asyncio.Queue = asyncio.Queue()

    ws = web.WebSocketResponse()
    await ws.prepare(request)
    agents[agent_id] = {"ws": ws, "name": name, "connected_at": time.time(), "queue": queue}
    log.info("Agent registered: %s (%s)", name, agent_id)

    await ws.send_str(json.dumps({"type": "registered", "id": agent_id, "name": name}))

    try:
        # Sole reader of agent_ws — puts messages into queue for client
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                await queue.put(("text", msg.data))
            elif msg.type == WSMsgType.BINARY:
                await queue.put(("binary", msg.data))
            elif msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.CLOSED):
                break
    finally:
        await queue.put(("close", ""))
        agents.pop(agent_id, None)
        log.info("Agent disconnected: %s (%s)", name, agent_id)

    return ws


async def handle_client_ws(request: web.Request) -> web.WebSocketResponse:
    """Browser client connects here to chat with an agent."""
    password = request.query.get("password", "")
    if password != request.app["client_password"]:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.close(code=4001, reason="bad password")
        return ws

    agent_id = request.query.get("agent", "")
    agent = agents.get(agent_id)
    if not agent:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.close(code=4004, reason="agent not found")
        return ws

    agent_ws = agent["ws"]
    queue = agent["queue"]
    client = web.WebSocketResponse()
    await client.prepare(request)
    log.info("Client connected to agent %s (%s)", agent["name"], agent_id)

    # Notify agent
    fresh = request.query.get("fresh", "")
    await agent_ws.send_str(json.dumps({"type": "client_connect", "fresh": fresh == "1"}))

    async def relay_agent_to_client():
        """Read from agent queue, send to client."""
        while True:
            kind, data = await queue.get()
            if kind == "text":
                await client.send_str(data)
            elif kind == "binary":
                await client.send_bytes(data)
            elif kind == "close":
                await client.close(code=1011, reason="agent disconnected")
                break

    async def relay_client_to_agent():
        """Read from client, send to agent."""
        async for msg in client:
            if msg.type == WSMsgType.TEXT:
                await agent_ws.send_str(json.dumps({"type": "data", "data": msg.data}))
            elif msg.type == WSMsgType.BINARY:
                await agent_ws.send_bytes(msg.data)
            elif msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.CLOSED):
                break

    try:
        done, pending = await asyncio.wait(
            [asyncio.create_task(relay_agent_to_client()), asyncio.create_task(relay_client_to_agent())],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for t in pending:
            t.cancel()
    finally:
        try:
            await agent_ws.send_str(json.dumps({"type": "client_disconnect"}))
        except Exception:
            pass
        log.info("Client disconnected from agent %s", agent_id)

    return client


async def run_server(host: str, port: int, client_password: str, agent_secret: str, ssl_cert: str = "", ssl_key: str = ""):
    app = web.Application()
    app["client_password"] = client_password
    app["agent_secret"] = agent_secret
    app.router.add_get("/", handle_index)
    app.router.add_get("/agents", handle_agents)
    app.router.add_get("/ws/agent", handle_agent_ws)
    app.router.add_get("/ws/client", handle_client_ws)

    runner = web.AppRunner(app)
    await runner.setup()

    ssl_ctx = None
    if ssl_cert and ssl_key:
        import ssl
        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ssl_ctx.load_cert_chain(ssl_cert, ssl_key)

    site = web.TCPSite(runner, host, port, ssl_context=ssl_ctx)
    await site.start()
    proto = "https" if ssl_ctx else "http"
    log.info("Server at %s://%s:%d", proto, host, port)
    log.info("Client password: %s", client_password)
    log.info("Agent secret: %s", agent_secret)

    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        await runner.cleanup()


def main():
    parser = argparse.ArgumentParser(description="Hermes Relay Server")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--password", default="", help="Client access password")
    parser.add_argument("--agent-secret", default="", help="Agent registration secret")
    parser.add_argument("--ssl-cert", default="", help="SSL cert file (enables WSS)")
    parser.add_argument("--ssl-key", default="", help="SSL key file")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    if not args.password:
        args.password = secrets.token_urlsafe(8)
        log.info("Auto client password: %s", args.password)
    if not args.agent_secret:
        args.agent_secret = secrets.token_urlsafe(16)
        log.info("Auto agent secret: %s", args.agent_secret)

    asyncio.run(run_server(args.host, args.port, args.password, args.agent_secret, args.ssl_cert, args.ssl_key))


if __name__ == "__main__":
    main()
