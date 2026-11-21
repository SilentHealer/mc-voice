// MC Voice relay (WebSocket). Rooms must be CREATED first; others JOIN with the code.
// Every audio frame is tagged with the speaker's id so phones can mix voices properly.
const http = require('http');
const { WebSocketServer } = require('ws');
const crypto = require('crypto');

const server = http.createServer((req, res) => res.end('MC voice relay v8 (rooms + names + profiles + world distance volume)'));
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

// ---------------------------------------------------------------------------
// Minecraft world link. The world HOST types   /wsserver wss://<this server>/mc/<room>
// in game chat. The game then connects here, and we ask it where every player is
// (querytarget), work out distances and tell each phone how loud each friend should be.
// Voice usernames must match Minecraft names for this to find people.
// ---------------------------------------------------------------------------
function distOf(a, b) {
  return Math.sqrt((a.x - b.x) ** 2 + (a.y - b.y) ** 2 + (a.z - b.z) ** 2);
}

function computeGains(r) {
  const now = Date.now();
  const fresh = new Map();
  for (const [id, p] of r.pos) if (now - p.t < 4000) fresh.set(id, p);

  const unmatched = [];
  for (const [id, c] of r.members) if (!fresh.has(id)) unmatched.push(c.name || 'Player');
  const un = 'U:' + unmatched.join(',');
  if (un !== r.lastU) { r.lastU = un; toAll(r, un); }

  for (const [lid, listener] of r.members) {
    const parts = [];
    const lp = fresh.get(lid);
    for (const sid of r.members.keys()) {
      if (sid === lid) continue;
      const sp = fresh.get(sid);
      let g = 1; // unknown position = normal volume
      if (r.prox && lp && sp) {
        if (lp.dim !== sp.dim) {
          g = 0;
        } else {
          const d = distOf(lp, sp);
          const full = 3; // closer than this = full volume
          g = d <= full ? 1 : Math.max(0, 1 - (d - full) / Math.max(1, r.range - full));
          g = g * g; // fades out smoothly
        }
      }
      parts.push(sid + '=' + Math.round(g * 100));
    }
    const msg = 'G:' + parts.join(',');
    if (r.lastG.get(lid) !== msg) {
      r.lastG.set(lid, msg);
      if (listener.readyState === 1) listener.send(msg);
    }
  }
}

// Print a line in the Minecraft chat of the player who linked the world.
function gameSay(ws, text) {
  if (ws.readyState !== 1) return;
  ws.send(JSON.stringify({
    header: { version: 1, requestId: crypto.randomUUID(), messageType: 'commandRequest', messagePurpose: 'commandRequest' },
    body: { version: 1, commandLine: 'tellraw @s {"rawtext":[{"text":' + JSON.stringify(text) + '}]}', origin: { type: 'player' } },
  }));
}

function handleGame(ws, room) {
  ws.on('error', () => {});
  const r = rooms.get(room);
  if (!r) {
    console.log('world link refused: there is no room "' + room + '"');
    gameSay(ws, '\u00a7c[Voice] Room "' + room + '" does not exist (yet). Create or join it in the voice app, keep the app open, then run the command again.');
    setTimeout(() => { try { ws.close(); } catch (e) { } }, 2000);
    return;
  }
  if (r.game && r.game.readyState === 1) r.game.close();
  r.game = ws;
  toAll(r, 'W:1');
  console.log('world linked to room "' + room + '"');
  gameSay(ws, '\u00a7a[Voice] World linked to room ' + room.toUpperCase() + '. Distance volume is ' + (r.prox ? 'on' : 'off') + '.');
  const keepAlive = setInterval(() => { try { if (ws.readyState === 1) ws.ping(); } catch (e) { } }, 20000);
  const send = (o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };

  const timer = setInterval(() => {
    if (r.pending.size > 300) r.pending.clear();
    if (r.prox) {
      for (const [id, c] of r.members) {
        const rid = crypto.randomUUID();
        r.pending.set(rid, id);
        send({
          header: { version: 1, requestId: rid, messageType: 'commandRequest', messagePurpose: 'commandRequest' },
          body: { version: 1, commandLine: 'querytarget @a[name="' + (c.name || 'Player') + '"]', origin: { type: 'player' } },
        });
      }
    }
    computeGains(r);
  }, 700);

  ws.on('message', (data) => {
    try {
      const m = JSON.parse(data.toString());
      if (m.header && m.header.messagePurpose === 'commandResponse') {
        const id = r.pending.get(m.header.requestId);
        r.pending.delete(m.header.requestId);
        if (id === undefined) return;
        if (m.body && m.body.statusCode === 0 && m.body.details) {
          const arr = JSON.parse(m.body.details);
          const p = arr && arr[0] && arr[0].position;
          if (p) r.pos.set(id, { x: p.x, y: p.y, z: p.z, dim: arr[0].dimension, t: Date.now() });
        }
      }
    } catch (e) { /* ignore anything we don't understand */ }
  });

  ws.on('close', () => {
    clearInterval(timer);
    clearInterval(keepAlive);
    console.log('world link closed for room "' + room + '"');
    if (r.game === ws) {
      r.game = null;
      r.pos.clear();
      r.lastG.clear();
      r.lastU = '';
      toAll(r, 'W:0');
      toAll(r, 'U:');
      toAll(r, 'G:'); // back to normal volume
    }
  });
}

wss.on('connection', (ws, req) => {
  const u = new URL(req.url, 'http://x');
  if (u.pathname.startsWith('/mc/')) { // the Minecraft game linking a world
    ws.isGame = true;
    handleGame(ws, decodeURIComponent(u.pathname.slice(4)).toLowerCase());
    return;
  }
  const room = (u.searchParams.get('room') || '').toLowerCase();
  const action = u.searchParams.get('action') === 'create' ? 'create' : 'join';
  const fail = (msg) => { ws.send(msg); ws.close(); };

  if (!CODE.test(room)) return fail('ERR:BADCODE');

  let r = rooms.get(room);
  if (action === 'create') {
    if (r && r.members.size > 0) return fail('ERR:EXISTS');
    if (!r) {
      r = { members: new Map(), timer: null, prox: true, range: 32, game: null,
            pos: new Map(), pending: new Map(), lastG: new Map(), lastU: '' };
      rooms.set(room, r);
    }
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
  ws.send('X:' + (r.prox ? 1 : 0) + ':' + r.range);
  ws.send('W:' + (r.game ? 1 : 0));
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
      } else if (t === 'prox:1' || t === 'prox:0') { // distance volume on/off
        r.prox = t === 'prox:1';
        toAll(r, 'X:' + (r.prox ? 1 : 0) + ':' + r.range);
        if (!r.prox) { r.lastG.clear(); toAll(r, 'G:'); }
      } else if (t.startsWith('range:')) { // hearing range in blocks
        const n = parseInt(t.slice(6), 10);
        if (n >= 4 && n <= 128) { r.range = n; toAll(r, 'X:' + (r.prox ? 1 : 0) + ':' + r.range); }
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
    if (r.members.get(id) === ws) { r.members.delete(id); r.pos.delete(id); r.lastG.delete(id); }
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
    if (ws.isGame) continue; // the game link has its own lifetime
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
}, 15000);

server.listen(process.env.PORT || 8080, () => console.log('Voice relay running'));
