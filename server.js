// WebSocket voice relay. Works on any host that supports Node + HTTPS
// (Render, Railway, Fly.io, a VPS...). Friends in the same room code hear each other.
const http = require('http');
const { WebSocketServer } = require('ws');

const server = http.createServer((req, res) => res.end('MC voice relay OK'));
const wss = new WebSocketServer({ server, maxPayload: 16 * 1024 });
const rooms = new Map();

wss.on('connection', (ws, req) => {
  const room = new URL(req.url, 'http://x').searchParams.get('room') || 'default';
  if (!rooms.has(room)) rooms.set(room, new Set());
  const set = rooms.get(room);
  set.add(ws);
  ws.isAlive = true;
  ws.on('pong', () => (ws.isAlive = true));

  ws.on('message', (data, isBinary) => {
    if (!isBinary) return;
    for (const c of set) if (c !== ws && c.readyState === 1) c.send(data, { binary: true });
  });
  ws.on('close', () => {
    set.delete(ws);
    if (!set.size) rooms.delete(room);
  });
});

// heartbeat: drops dead connections and keeps proxies from closing idle ones
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
}, 25000);

server.listen(process.env.PORT || 8080, () => console.log('Voice relay running'));
