const express = require('express');
const http = require('http');
const path = require('path');
const crypto = require('crypto');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 1e6 });
app.use(express.static(path.join(__dirname, 'public')));

const rooms = new Map(); // code -> { code, players:{P1,P2}, spectators:{token:{...}}, state, chat, touched }
const ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const RELAY = new Set(['sync', 'handoff', 'attack_done', 'log', 'bg']);
const MAX_SPECTATORS = 10;

const newCode = () => {
  let c;
  do { c = Array.from({ length: 5 }, () => ALPHABET[crypto.randomInt(ALPHABET.length)]).join(''); }
  while (rooms.has(c));
  return c;
};
const cleanNick = (n) => String(n ?? '').replace(/[\u0000-\u001f<>&"'`\\]/g, '').replace(/\s+/g, ' ').trim().slice(0, 16);
const started = (r) => !!(r.players.P1 && r.players.P2);
const publicRoom = (r) => ({
  nicks: { P1: r.players.P1?.nick, P2: r.players.P2?.nick },
  connected: { P1: !!r.players.P1?.sid, P2: !!r.players.P2?.sid },
  started: started(r),
  spectators: Object.values(r.spectators).filter((s) => s.sid).map((s) => s.nick),
});
const broadcast = (r) => io.to(r.code).emit('room', publicRoom(r));
const whoIs = (room, role, token) => (role === 'S' ? room.spectators[token] : room.players[role]);

io.on('connection', (socket) => {
  const attach = (room, role, who) => {
    who.sid = socket.id; who.off = null;
    room.touched = Date.now();
    socket.data = { code: room.code, role, token: who.token };
    socket.join(room.code);
  };

  socket.on('create_room', (data, ack) => {
    const nick = cleanNick(data?.nick);
    if (!nick) return ack?.({ ok: false, error: 'Enter a nickname.' });
    const room = { code: newCode(), players: {}, spectators: {}, state: null, bg: null, chat: [], touched: Date.now() };
    const token = crypto.randomBytes(16).toString('hex');
    room.players.P1 = { nick, token, sid: null };
    rooms.set(room.code, room);
    attach(room, 'P1', room.players.P1);
    ack?.({ ok: true, code: room.code, role: 'P1', token });
    broadcast(room);
  });

  socket.on('join_room', (data, ack) => {
    const nick = cleanNick(data?.nick);
    const room = rooms.get(String(data?.code ?? '').trim().toUpperCase());
    if (!nick) return ack?.({ ok: false, error: 'Enter a nickname.' });
    if (!room) return ack?.({ ok: false, error: 'Room not found. Check the code.' });
    const token = crypto.randomBytes(16).toString('hex');
    if (data?.spectate) {
      if (Object.keys(room.spectators).length >= MAX_SPECTATORS) return ack?.({ ok: false, error: 'Spectator limit reached.' });
      room.spectators[token] = { nick, token, sid: null };
      attach(room, 'S', room.spectators[token]);
      ack?.({ ok: true, code: room.code, role: 'S', token });
      return broadcast(room);
    }
    if (room.players.P2) return ack?.({ ok: false, error: 'This room is already full. You can watch it as a spectator.' });
    room.players.P2 = { nick, token, sid: null };
    attach(room, 'P2', room.players.P2);
    ack?.({ ok: true, code: room.code, role: 'P2', token });
    broadcast(room);
  });

  socket.on('rejoin', (data, ack) => {
    const room = rooms.get(String(data?.code ?? '').toUpperCase());
    if (!room) return ack?.({ ok: false });
    let role = ['P1', 'P2'].find((r) => room.players[r]?.token === data?.token);
    let who = role && room.players[role];
    if (!role && room.spectators[data?.token]) { role = 'S'; who = room.spectators[data.token]; }
    if (!role) return ack?.({ ok: false });
    attach(room, role, who);
    ack?.({ ok: true, role, state: room.state, bg: room.bg, chat: room.chat, ...publicRoom(room) });
    broadcast(room);
  });

  socket.on('leave_room', () => {
    const { code, role, token } = socket.data || {};
    const room = rooms.get(code);
    if (!room) return;
    socket.leave(code);
    socket.data = {};
    if (role === 'S') delete room.spectators[token];
    else if (!started(room)) rooms.delete(code);
    else room.players[role].sid = null;
    if (rooms.has(code)) broadcast(room);
  });

  // Only the two players may drive the game; spectators are receive-only
  socket.on('msg', (m) => {
    const { code, role } = socket.data || {};
    const room = rooms.get(code);
    if (!room || !started(room) || (role !== 'P1' && role !== 'P2') || !RELAY.has(m?.type)) return;
    if (m.type === 'sync') {
      if (typeof m.payload !== 'string' || m.payload.length > 600000) return;
      room.state = m.payload;
    }
    if (m.type === 'bg') {
      const p = m.payload;
      if (role !== 'P1' || (p && (typeof p.data !== 'string' || p.data.length > 900000 || !/^data:image\/(jpeg|png|webp);base64,/.test(p.data)))) return;
      room.bg = p || null;
    }
    room.touched = Date.now();
    socket.to(code).emit('msg', { type: m.type, payload: m.payload });
  });

  socket.on('chat', (text) => {
    const { code, role, token } = socket.data || {};
    const room = rooms.get(code);
    if (!room || typeof text !== 'string') return;
    const now = Date.now();
    if (now - (socket.data.lastChat || 0) < 400) return; // simple rate limit
    socket.data.lastChat = now;
    const msg = text.replace(/[\u0000-\u001f]/g, ' ').trim().slice(0, 200);
    if (!msg) return;
    const entry = { from: whoIs(room, role, token)?.nick || '?', role, text: msg };
    room.chat.push(entry);
    if (room.chat.length > 50) room.chat.shift();
    io.to(code).emit('chat', entry);
  });

  socket.on('disconnect', () => {
    const { code, role, token } = socket.data || {};
    const room = rooms.get(code);
    const who = room && whoIs(room, role, token);
    if (who && who.sid === socket.id) {
      who.sid = null; who.off = Date.now();
      room.touched = Date.now();
      broadcast(room);
    }
  });
});

setInterval(() => {
  const now = Date.now();
  for (const [code, r] of rooms) {
    for (const [t, s] of Object.entries(r.spectators)) if (!s.sid && now - (s.off || 0) > 120e3) delete r.spectators[t];
    if (!r.players.P1?.sid && !r.players.P2?.sid && now - r.touched > 30 * 60e3) rooms.delete(code);
  }
}, 60e3);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Kill Team VTT running on http://localhost:${PORT}`));
