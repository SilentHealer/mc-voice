// MC Voice relay (WebSocket). Rooms must be CREATED first; others JOIN with the code.
// Every audio frame is tagged with the speaker's id so phones can mix voices properly.
const http = require('http');
const { WebSocketServer } = require('ws');

const server = http.createServer((req, res) => res.end('MC voice relay v4 (rooms + names)'));
const wss = new WebSocketServer({ server, maxPayload: 16 * 1024 });
const rooms = new Map(); // code -> { members: Map(id -> ws), timer }
const CODE = /^[a-z0-9_-]{3,20}$/;
const EMPTY_ROOM_GRACE_MS = 2 * 60 * 1000; // survive short reconnects

function broadcastCount(r) {
  const list = [...r.members].map(([i, c]) => ({ i, n: c.name || 'Player' }));
  const msg = 'L:' + JSON.stringify(list); // who is in the room
  for (const c of r.members.values()) {
    if (c.readyState === 1) { c.send('N:' + r.members.size); c.send(msg); }
  }
}

wss.on('connection', (ws, req) => {
  const u = new URL(req.url, 'http://x');
  const room = (u.searchParams.get('room') || '').toLowerCase();
  const action = u.searchParams.get('action') === 'create' ? 'create' : 'join';
  const fail = (msg) => { ws.send(msg); ws.close(); };

  if (!CODE.test(room)) return fail('ERR:BADCODE');

  let r = rooms.get(room);
  if (action === 'create') {
    if (r && r.members.size > 0) return fail('ERR:EXISTS');
    if (!r) { r = { members: new Map(), timer: null }; rooms.set(room, r); }
  } else if (!r) {
    return fail('ERR:NOROOM');
  }
  if (r.timer) { clearTimeout(r.timer); r.timer = null; }

  // One connection per phone: if the same phone reconnects, drop its stale old connection.
  const cid = (u.searchParams.get('cid') || '').slice(0, 40);
  if (cid) {
    for (const [oid, oc] of r.members) {
      if (oc.cid === cid) { r.members.delete(oid); oc.terminate(); }
    }
  }
  ws.cid = cid;
  ws.name = (u.searchParams.get('name') || 'Player').replace(/[^\w .-]/g, '').trim().slice(0, 16) || 'Player';

  let id = -1;
  for (let i = 0; i < 255; i++) if (!r.members.has(i)) { id = i; break; }
  if (id < 0) return fail('ERR:FULL');

  ws.deaf = false;
  ws.isAlive = true;
  ws.on('pong', () => (ws.isAlive = true));
  r.members.set(id, ws);
  ws.send('OK:' + id);
  broadcastCount(r);

  ws.on('message', (data, isBinary) => {
    if (!isBinary) { // control messages
      const t = data.toString();
      if (t === 'deaf:1') ws.deaf = true;
      else if (t === 'deaf:0') ws.deaf = false;
      return;
    }
    const out = Buffer.allocUnsafe(data.length + 1);
    out[0] = id;
    data.copy(out, 1);
    for (const c of r.members.values()) {
      if (c !== ws && !c.deaf && c.readyState === 1) c.send(out, { binary: true });
    }
  });

  ws.on('close', () => {
    if (r.members.get(id) === ws) r.members.delete(id);
    broadcastCount(r);
    if (r.members.size === 0 && rooms.get(room) === r) {
      r.timer = setTimeout(() => {
        if (r.members.size === 0 && rooms.get(room) === r) rooms.delete(room);
      }, EMPTY_ROOM_GRACE_MS);
    }
  });
});

setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
}, 15000);

server.listen(process.env.PORT || 8080, () => console.log('Voice relay running'));
