const { WebSocketServer } = require('ws');
const http = require('http');

const PORT = process.env.PORT || 3000;
const server = http.createServer((req, res) => {
  // Health check
  res.writeHead(200, { 
    'Content-Type': 'text/plain',
    'Access-Control-Allow-Origin': '*'
  });
  res.end('poker-ws ok');
});

const wss = new WebSocketServer({ 
  server,
  // Keep connections alive
  clientTracking: true,
  perMessageDeflate: false
});

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
    try {
      ws.send(JSON.stringify({ type: 'state', state: stateForPlayer(state, playerId) }));
    } catch(e) {}
  });
}

// Server-side heartbeat to detect dead connections
const HEARTBEAT_INTERVAL = 20000; // 20s
setInterval(() => {
  wss.clients.forEach(ws => {
    if (ws.isAlive === false) {
      ws.terminate();
      return;
    }
    ws.isAlive = false;
    ws.ping();
  });
}, HEARTBEAT_INTERVAL);

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  let currentRoom = null;

  ws.on('message', (raw) => {
    ws.isAlive = true; // mark alive on any message
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    const { type, roomId, playerId, state, emote } = msg;

    if (type === 'join') {
      currentRoom = roomId;
      const room = getRoom(roomId);
      room.clients.set(ws, playerId);
      if (!room.state && state) room.state = state;
      if (room.state) {
        try {
          ws.send(JSON.stringify({ 
            type: 'state', 
            state: stateForPlayer(room.state, playerId) 
          }));
        } catch(e) {}
      }
      console.log(`[${roomId}] +join pid=${playerId} total=${room.clients.size}`);
    }

    else if (type === 'state_update') {
      if (!currentRoom) return;
      const room = rooms.get(currentRoom); if (!room) return;
      if (!room.state || state.ts >= (room.state.ts || 0)) {
        // Merge hands: preserve real cards from old state where client may have sent null
        // BUT ONLY within the same round + street (don't carry over to new round/street)
        const sameRound = room.state && room.state.round === state.round
                        && room.state.phase !== 'showdown'  // showdown→playing means new round
                        && state.phase === 'playing';
        if (sameRound && room.state.players) {
          state.players = state.players.map(newP => {
            const oldP = room.state.players.find(op => op.id === newP.id);
            if (!oldP || !oldP.hand || !newP.hand) return newP;
            // Only restore null slots that were ALSO non-null in old state
            newP.hand = newP.hand.map((c, ci) => {
              if (c === null && oldP.hand[ci]) {
                // But only if old state's street had this slot dealt
                // (slots [0,1] dealt at street 1, [2] at street 2, [3] at street 3, [4] at street 4)
                const slotStreet = (ci === 0 || ci === 1) ? 1 : (ci === 2 ? 2 : (ci === 3 ? 3 : 4));
                if (slotStreet <= (room.state.street || 1)) return oldP.hand[ci];
              }
              return c;
            });
            return newP;
          });
        }
        room.state = state;
        broadcastAll(room, state);
      }
    }

    else if (type === 'emote') {
      if (!currentRoom) return;
      const room = rooms.get(currentRoom); if (!room) return;
      room.clients.forEach((pid, ws2) => {
        if (ws2.readyState === 1) {
          try { ws2.send(JSON.stringify({ type: 'emote', playerId, emote })); } catch(e) {}
        }
      });
    }

    else if (type === 'ping') {
      try { ws.send(JSON.stringify({ type: 'pong', ts: msg.ts })); } catch(e) {}
    }
  });

  ws.on('close', () => {
    if (currentRoom) {
      const room = rooms.get(currentRoom);
      if (room) {
        room.clients.delete(ws);
        console.log(`[${currentRoom}] -leave total=${room.clients.size}`);
        if (room.clients.size === 0) {
          setTimeout(() => {
            const r = rooms.get(currentRoom);
            if (r && r.clients.size === 0) rooms.delete(currentRoom);
          }, 30 * 60 * 1000);
        }
      }
    }
  });

  ws.on('error', (e) => { console.error('ws error:', e.message); });
});

server.listen(PORT, () => console.log(`poker-ws on ${PORT}`));
