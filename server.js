const { WebSocketServer } = require('ws');
const http = require('http');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('poker-ws ok');
});
const wss = new WebSocketServer({ server });

// rooms: { roomId: { state, clients: Map<playerId, ws> } }
const rooms = new Map();

function getRoom(roomId) {
  if (!rooms.has(roomId)) rooms.set(roomId, { state: null, clients: new Map() });
  return rooms.get(roomId);
}

// Strip private hand data — only send a player their own hole cards
// Hole card indices: 0 and 4 (dark cards)
function stateForPlayer(state, playerId) {
  if (!state) return null;
  const s = JSON.parse(JSON.stringify(state));
  s.players = s.players.map(p => {
    if (p.id === playerId) return p; // own full hand
    // Mask hole cards for others
    if (p.hand) {
      p.hand = p.hand.map((c, ci) => {
        const isHole = ci === 0 || ci === 4;
        // Reveal hole cards only at showdown (and only if not folded)
        if (isHole && state.phase !== 'showdown') return null;
        if (isHole && state.phase === 'showdown' && p.folded) return null;
        return c;
      });
    }
    return p;
  });
  return s;
}

function broadcast(room, msg, excludeId = null) {
  room.clients.forEach((ws, pid) => {
    if (pid === excludeId) return;
    if (ws.readyState !== 1) return;
    // Send state filtered per player
    if (msg.type === 'state' && msg.state) {
      ws.send(JSON.stringify({ type: 'state', state: stateForPlayer(msg.state, pid) }));
    } else {
      ws.send(JSON.stringify(msg));
    }
  });
}

function broadcastAll(room, msg) {
  broadcast(room, msg, null);
}

function sendTo(ws, playerId, msg) {
  if (ws.readyState !== 1) return;
  if (msg.type === 'state' && msg.state) {
    ws.send(JSON.stringify({ type: 'state', state: stateForPlayer(msg.state, playerId) }));
  } else {
    ws.send(JSON.stringify(msg));
  }
}

wss.on('connection', (ws) => {
  let currentRoom = null;
  let currentPlayerId = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    const { type, roomId, playerId, state, emote } = msg;

    if (type === 'join') {
      currentRoom = roomId;
      currentPlayerId = playerId;
      const room = getRoom(roomId);
      room.clients.set(playerId, ws);
      // If this is the first join and state provided, initialise room
      if (!room.state && state) room.state = state;
      // Send current state to joiner (their own view)
      if (room.state) sendTo(ws, playerId, { type: 'state', state: room.state });
      console.log(`[${roomId}] join pid=${playerId} total=${room.clients.size}`);
    }

    else if (type === 'state_update') {
      if (!currentRoom) return;
      const room = rooms.get(currentRoom); if (!room) return;
      if (!room.state || state.ts >= (room.state.ts || 0)) {
        room.state = state;
        broadcastAll(room, { type: 'state', state });
      }
    }

    else if (type === 'emote') {
      if (!currentRoom) return;
      const room = rooms.get(currentRoom); if (!room) return;
      broadcastAll(room, { type: 'emote', playerId: currentPlayerId, emote });
    }

    else if (type === 'ping') {
      ws.send(JSON.stringify({ type: 'pong', ts: msg.ts }));
    }
  });

  ws.on('close', () => {
    if (currentRoom) {
      const room = rooms.get(currentRoom);
      if (room) {
        room.clients.delete(currentPlayerId);
        console.log(`[${currentRoom}] leave pid=${currentPlayerId} total=${room.clients.size}`);
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
