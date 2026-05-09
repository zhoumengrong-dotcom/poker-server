const { WebSocketServer } = require('ws');
const http = require('http');

const PORT = process.env.PORT || 3000;
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('poker-ws ok');
});
const wss = new WebSocketServer({ server });

// rooms: { roomId: { state, clients: Map<ws, playerId> } }
const rooms = new Map();

function getRoom(roomId) {
  if (!rooms.has(roomId)) rooms.set(roomId, { state: null, clients: new Map() });
  return rooms.get(roomId);
}

function stateForPlayer(state, playerId) {
  if (!state) return null;
  const s = JSON.parse(JSON.stringify(state));
  s.players = s.players.map(p => {
    if (p.id === playerId) return p;
    if (p.hand) {
      p.hand = p.hand.map((c, ci) => {
        const isHole = ci === 0 || ci === 4;
        if (isHole && state.phase !== 'showdown') return null;
        if (isHole && state.phase === 'showdown' && p.folded) return null;
        return c;
      });
    }
    return p;
  });
  return s;
}

function broadcastAll(room, state) {
  room.clients.forEach((playerId, ws) => {
    if (ws.readyState !== 1) return;
    ws.send(JSON.stringify({ type: 'state', state: stateForPlayer(state, playerId) }));
  });
}

wss.on('connection', (ws) => {
  let currentRoom = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    const { type, roomId, playerId, state, emote } = msg;

    if (type === 'join') {
      currentRoom = roomId;
      const room = getRoom(roomId);
      // Register this ws with its playerId
      room.clients.set(ws, playerId);
      // If room has no state yet and joiner provides one, use it
      if (!room.state && state) room.state = state;
      // Send current state to joiner
      if (room.state) {
        ws.send(JSON.stringify({ type: 'state', state: stateForPlayer(room.state, playerId) }));
      }
      console.log(`[${roomId}] join pid=${playerId} total=${room.clients.size}`);
    }

    else if (type === 'state_update') {
      if (!currentRoom) return;
      const room = rooms.get(currentRoom); if (!room) return;
      if (!room.state || state.ts >= (room.state.ts || 0)) {
        room.state = state;
        broadcastAll(room, state);
      }
    }

    else if (type === 'emote') {
      if (!currentRoom) return;
      const room = rooms.get(currentRoom); if (!room) return;
      room.clients.forEach((pid, ws2) => {
        if (ws2.readyState === 1) {
          ws2.send(JSON.stringify({ type: 'emote', playerId, emote }));
        }
      });
    }

    else if (type === 'ping') {
      ws.send(JSON.stringify({ type: 'pong', ts: msg.ts }));
    }
  });

  ws.on('close', () => {
    if (currentRoom) {
      const room = rooms.get(currentRoom);
      if (room) {
        room.clients.delete(ws);
        console.log(`[${currentRoom}] leave total=${room.clients.size}`);
        if (room.clients.size === 0) {
          setTimeout(() => {
            const r = rooms.get(currentRoom);
            if (r && r.clients.size === 0) rooms.delete(currentRoom);
          }, 30 * 60 * 1000);
        }
      }
    }
  });

  ws.on('error', () => {});
});

server.listen(PORT, () => console.log(`poker-ws on ${PORT}`));
