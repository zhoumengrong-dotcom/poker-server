const { WebSocketServer } = require('ws');
const http = require('http');

const PORT = process.env.PORT || 3000;
const server = http.createServer((req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/plain',
    'Access-Control-Allow-Origin': '*'
  });
  res.end('poker-ws ok');
});

const wss = new WebSocketServer({
  server,
  clientTracking: true,
  perMessageDeflate: false
});

const rooms = new Map();

function getRoom(roomId) {
  if (!rooms.has(roomId)) {
    rooms.set(roomId, { state: null, clients: new Map(), trueHands: {}, roundId: 0 });
  }
  return rooms.get(roomId);
}

// stateForPlayer: returns a copy of state with other players' hole cards filtered
function stateForPlayer(state, playerId) {
  if (!state) return null;
  const s = JSON.parse(JSON.stringify(state));
  s.players = s.players.map(p => {
    if (p.id === playerId) return p; // self — full info
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

// Update trueHands from a state: for each player, store any real (non-null) cards
function updateTrueHands(room, state) {
  if (!state || !state.players) return;
  if (!room.trueHands) room.trueHands = {};
  state.players.forEach(p => {
    if (!p.id || !p.hand) return;
    if (!room.trueHands[p.id]) room.trueHands[p.id] = [null, null, null, null, null];
    p.hand.forEach((c, ci) => {
      if (c) room.trueHands[p.id][ci] = c; // record any non-null card we see
    });
  });
}

// Reconstruct full state by patching in trueHands for any null slots
function reconstructState(room, state) {
  if (!state || !state.players || !room.trueHands) return state;
  state.players.forEach(p => {
    const trueHand = room.trueHands[p.id];
    if (!trueHand || !p.hand) return;
    p.hand = p.hand.map((c, ci) => c !== null && c !== undefined ? c : trueHand[ci]);
  });
  return state;
}

const HEARTBEAT_INTERVAL = 20000;
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
    ws.isAlive = true;
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    const { type, roomId, playerId, state, emote } = msg;

    if (type === 'join') {
      currentRoom = roomId;
      const room = getRoom(roomId);
      room.clients.set(ws, playerId);
      if (!room.state && state) {
        room.state = state;
        updateTrueHands(room, state);
      }
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

      // Reject stale states (race condition: client acted on outdated state)
      // pot/street can only progress, not regress, within same round
      if (room.state
          && state.round === room.state.round
          && state.phase === 'playing' && room.state.phase === 'playing') {
        if ((state.street || 1) < (room.state.street || 1)) {
          console.log('[stale] street regression', state.street, '<', room.state.street);
          // Resend authoritative state to this client
          try { ws.send(JSON.stringify({ type: 'state', state: stateForPlayer(room.state, playerId) })); } catch(e) {}
          return;
        }
        if ((state.street || 1) === (room.state.street || 1) && (state.pot || 0) < (room.state.pot || 0)) {
          console.log('[stale] pot regression', state.pot, '<', room.state.pot);
          try { ws.send(JSON.stringify({ type: 'state', state: stateForPlayer(room.state, playerId) })); } catch(e) {}
          return;
        }
      }

      // Detect new round: round number incremented, or new game starts (phase change)
      const isNewRound = !room.state
        || (state.round && room.state.round && state.round > room.state.round)
        || (state.phase === 'playing' && room.state.phase === 'showdown')
        || (state.phase === 'lobby');

      if (isNewRound) {
        // New round: reset trueHands, accept the incoming state as fresh truth
        room.trueHands = {};
      }

      // Update trueHands: record any new real cards seen in this state
      updateTrueHands(room, state);

      // Reconstruct: fill in any nulls in the state with our trueHands record
      const fullState = reconstructState(room, state);

      room.state = fullState;
      broadcastAll(room, fullState);
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
