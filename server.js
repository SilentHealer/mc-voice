// MC Voice relay (WebSocket). Rooms must be CREATED first; others JOIN with the code.
// Every audio frame is tagged with the speaker's id so phones can mix voices properly.
const http = require('http');
const { WebSocketServer } = require('ws');

const server = http.createServer((req, res) => res.end('MC voice relay v6 (rooms + names + profiles + echo test)'));
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

const MAX_AVATAR = 14000; // base64 characters (a 96x96 JPEG is ~5-9 KB)
const stateOf = (c) => (c.muted ? '1' : '0') + (c.deaf ? '1' : '0');
function toAll(r, msg) {
  for (const c of r.members.values()) if (c.readyState === 1) c.send(msg);
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
  ws.echo = false;
  ws.muted = false;
  ws.avatar = '';
  ws.isAlive = true;
  ws.on('pong', () => (ws.isAlive = true));
  r.members.set(id, ws);
  ws.send('OK:' + id);
  broadcastCount(r);
  // catch the newcomer up on everyone's mute/deafen state and profile picture...
  for (const [i, c] of r.members) {
    if (c === ws) continue;
    ws.send('S:' + i + ':' + stateOf(c));
    if (c.avatar) ws.send('A:' + i + ':' + c.avatar);
  }
  // ...and clear whatever an earlier member with the same id left behind on other phones
  for (const c of r.members.values()) {
    if (c !== ws && c.readyState === 1) { c.send('A:' + id + ':'); c.send('S:' + id + ':00'); }
  }

  ws.on('message', (data, isBinary) => {
    if (!isBinary) { // control messages
      const t = data.toString();
      if (t === 'deaf:1' || t === 'deaf:0') {
        ws.deaf = t === 'deaf:1';
        toAll(r, 'S:' + id + ':' + stateOf(ws));
      } else if (t === 'echo:1' || t === 'echo:0') { // "hear myself" test
        ws.echo = t === 'echo:1';
      } else if (t === 'mute:1' || t === 'mute:0') {
        ws.muted = t === 'mute:1';
        toAll(r, 'S:' + id + ':' + stateOf(ws));
      } else if (t.startsWith('P:')) { // profile picture (base64 JPEG, empty = remove)
        const b = t.slice(2);
        if (b.length <= MAX_AVATAR && /^[A-Za-z0-9+/=]*$/.test(b)) {
          ws.avatar = b;
          for (const c of r.members.values()) {
            if (c !== ws && c.readyState === 1) c.send('A:' + id + ':' + b);
          }
        }
      }
      return;
    }
    const out = Buffer.allocUnsafe(data.length + 1);
    out[0] = id;
    data.copy(out, 1);
    for (const c of r.members.values()) {
      if (c !== ws && !c.deaf && c.readyState === 1) c.send(out, { binary: true });
    }
    if (ws.echo && ws.readyState === 1) ws.send(out, { binary: true });
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
