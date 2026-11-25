// Липовый отличник — мультиплеер-сервер (вер. 0.11.0).
//
// WebSocket-ретранслятор комнат: сам игровой симулятор живёт у клиентов,
// сервер только держит лобби (код, публичность, роли, хоста) и пересылает
// сообщения между игроками одной комнаты. Так работает на любой площадке
// (itch / Яндекс / ВК), потому что браузеру нужен только WebSocket-клиент.
//
// Запуск:  node index.js   (PORT, иначе 8787)
// Хелсчек: GET /           -> "lipovy-mp-server ok"  (нужен бесплатным хостингам)

import { WebSocketServer } from 'ws';
import { createServer } from 'http';

const PORT = Number(process.env.PORT) || 8787;

// ---- лимиты (совпадают со спецификацией) ----
const MAX_PLAYERS = 11;
const MAX_TEACHERS = 2;
const MAX_STUDENTS = 9;
const NICK_MAX = 16;
const TEACHER_CHARS = ['rusichka', 'math', 'history'];

// ---- состояние ----
const lobbies = new Map();    // code -> lobby
const playerLobby = new Map(); // playerId -> code

let nextId = 1;
const genId = () => 'p' + (nextId++);
const genCode = () => {
  let code;
  do { code = String(Math.floor(1000 + Math.random() * 9000)); } while (lobbies.has(code));
  return code;
};

function send(ws, obj) {
  try { if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(obj)); } catch { /* ignore */ }
}

function roleOf(v) { return v === 'teacher' ? 'teacher' : 'student'; }

function roomState(lobby) {
  return {
    code: lobby.code,
    public: lobby.public,
    hostId: lobby.hostId,
    started: lobby.started,
    players: [...lobby.players.values()].map((p) => ({ id: p.id, nick: p.nick, role: p.role, char: p.char ?? null })),
  };
}

function broadcast(lobby, obj, exceptId = null) {
  for (const p of lobby.players.values()) {
    if (p.id === exceptId) continue;
    send(p.ws, obj);
  }
}

function countRole(lobby, role) {
  let n = 0;
  for (const p of lobby.players.values()) if (p.role === role) n++;
  return n;
}

function leave(playerId) {
  const code = playerLobby.get(playerId);
  if (!code) return;
  playerLobby.delete(playerId);
  const lobby = lobbies.get(code);
  if (!lobby) return;
  lobby.players.delete(playerId);
  if (lobby.players.size === 0) {
    lobbies.delete(code);
    return;
  }
  // хост ушёл — передаём роль следующему
  if (lobby.hostId === playerId) {
    const next = lobby.players.values().next().value;
    lobby.hostId = next.id;
  }
  broadcast(lobby, { type: 'room', state: roomState(lobby) });
}

// ---- HTTP: хелсчек ----
const http = createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('lipovy-mp-server ok');
});

const wss = new WebSocketServer({ server: http });

// сердцебиение: держим соединения живыми (бесплатные хостинги режут idle)
const HEARTBEAT_MS = 30000;
setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) { ws.terminate(); continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch { /* ignore */ }
  }
}, HEARTBEAT_MS);

wss.on('connection', (ws) => {
  ws.id = genId();
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    handle(ws, msg);
  });
  ws.on('close', () => leave(ws.id));
  ws.on('error', () => { /* закрытие обработает close */ });
});

function handle(ws, msg) {
  if (!msg || typeof msg.type !== 'string') return;

  switch (msg.type) {
    case 'create': {
      if (playerLobby.has(ws.id)) return send(ws, { type: 'error', message: 'Ты уже в лобби' });
      const code = genCode();
      const lobby = { code, public: !!msg.public, hostId: ws.id, started: false, players: new Map() };
      const player = { id: ws.id, ws, nick: cleanNick(msg.nick), role: roleOf(msg.role), char: null };
      lobby.players.set(ws.id, player);
      lobbies.set(code, lobby);
      playerLobby.set(ws.id, code);
      send(ws, { type: 'created', code, id: ws.id, host: true, state: roomState(lobby) });
      break;
    }

    case 'join': {
      if (playerLobby.has(ws.id)) return send(ws, { type: 'error', message: 'Ты уже в лобби' });
      const code = String(msg.code ?? '').trim();
      const lobby = lobbies.get(code);
      if (!lobby) return send(ws, { type: 'error', message: 'Лобби не найдено' });
      if (lobby.started) return send(ws, { type: 'error', message: 'Матч уже идёт' });
      if (lobby.players.size >= MAX_PLAYERS) return send(ws, { type: 'error', message: 'Лобби заполнено' });
      const role = roleOf(msg.role);
      const cap = role === 'teacher' ? MAX_TEACHERS : MAX_STUDENTS;
      if (countRole(lobby, role) >= cap) {
        return send(ws, { type: 'error', message: role === 'teacher' ? 'Учителей уже двое' : 'Учеников уже девять' });
      }
      const player = { id: ws.id, ws, nick: cleanNick(msg.nick), role, char: null };
      lobby.players.set(ws.id, player);
      playerLobby.set(ws.id, code);
      send(ws, { type: 'joined', id: ws.id, code, state: roomState(lobby) });
      broadcast(lobby, { type: 'room', state: roomState(lobby) }, ws.id);
      break;
    }

    case 'list': {
      const list = [...lobbies.values()]
        .filter((l) => l.public && !l.started && l.players.size < MAX_PLAYERS)
        .map((l) => ({ code: l.code, players: l.players.size }));
      send(ws, { type: 'lobbies', list });
      break;
    }

    case 'set_role': {
      const lobby = lobbyOf(ws);
      if (!lobby || lobby.started) return;
      const p = lobby.players.get(ws.id);
      if (!p) return;
      const role = roleOf(msg.role);
      if (role === p.role) return;
      const cap = role === 'teacher' ? MAX_TEACHERS : MAX_STUDENTS;
      if (countRole(lobby, role) >= cap) {
        return send(ws, { type: 'error', message: role === 'teacher' ? 'Учителей уже двое' : 'Учеников уже девять' });
      }
      p.role = role;
      if (role !== 'teacher') p.char = null; // персонаж нужен только учителю
      broadcast(lobby, { type: 'room', state: roomState(lobby) });
      break;
    }

    case 'set_char': {
      const lobby = lobbyOf(ws);
      if (!lobby || lobby.started) return;
      const p = lobby.players.get(ws.id);
      if (!p || p.role !== 'teacher') return;
      const char = String(msg.char ?? '').trim();
      if (!TEACHER_CHARS.includes(char)) return send(ws, { type: 'error', message: 'Нет такого персонажа' });
      for (const o of lobby.players.values()) {
        if (o.id !== ws.id && o.role === 'teacher' && o.char === char) {
          return send(ws, { type: 'error', message: 'Этот персонаж уже занят' });
        }
      }
      p.char = char;
      broadcast(lobby, { type: 'room', state: roomState(lobby) });
      break;
    }

    case 'kick': {
      const lobby = lobbyOf(ws);
      if (!lobby || lobby.hostId !== ws.id) return;
      const target = lobby.players.get(msg.id);
      if (!target || target.id === ws.id) return;
      send(target.ws, { type: 'kicked' });
      lobby.players.delete(target.id);
      playerLobby.delete(target.id);
      broadcast(lobby, { type: 'room', state: roomState(lobby) });
      break;
    }

    case 'start': {
      const lobby = lobbyOf(ws);
      if (!lobby || lobby.hostId !== ws.id || lobby.started) return;
      const players = [...lobby.players.values()];
      const teachers = players.filter((p) => p.role === 'teacher').length;
      const students = players.filter((p) => p.role === 'student').length;
      if (players.length < 2 || teachers < 1 || students < 1) {
        return send(ws, { type: 'error', message: 'Нужно минимум 2 игрока: ≥1 учитель и ≥1 ученик' });
      }
      lobby.started = true;
      broadcast(lobby, { type: 'started', state: roomState(lobby) });
      break;
    }

    case 'relay': {
      // переслать всем остальным в комнате (игровые события)
      const lobby = lobbyOf(ws);
      if (!lobby) return;
      broadcast(lobby, { type: 'relay', from: ws.id, data: msg.data }, ws.id);
      break;
    }

    case 'relay_to': {
      const lobby = lobbyOf(ws);
      if (!lobby) return;
      const target = lobby.players.get(msg.id);
      if (target) send(target.ws, { type: 'relay', from: ws.id, data: msg.data });
      break;
    }

    case 'vote': {
      // голосование в паузе: пересылаем остальным (итог считает хост)
      const lobby = lobbyOf(ws);
      if (!lobby) return;
      broadcast(lobby, { type: 'vote', from: ws.id, key: msg.key, value: msg.value }, ws.id);
      break;
    }

    case 'back_to_lobby': {
      // возврат всех в лобби (не распуская): сбрасываем флаг «идёт матч»
      const lobby = lobbyOf(ws);
      if (!lobby || lobby.hostId !== ws.id) return;
      lobby.started = false;
      broadcast(lobby, { type: 'room', state: roomState(lobby) });
      break;
    }

    case 'leave': {
      leave(ws.id);
      break;
    }

    default:
      break;
  }
}

function lobbyOf(ws) {
  const code = playerLobby.get(ws.id);
  return code ? lobbies.get(code) : null;
}

function cleanNick(nick) {
  const s = String(nick ?? '').trim().slice(0, NICK_MAX);
  return s || 'Игрок';
}

http.listen(PORT, () => {
  console.log(`[lipovy-mp-server] слушаю :${PORT} (лимит ${MAX_PLAYERS} игроков, ${MAX_TEACHERS} учителя, ${MAX_STUDENTS} учеников)`);
});
