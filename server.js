// MundoChat - servidor de señalización y emparejamiento (prototipo)
const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');

const app = express();
app.set('trust proxy', true);
app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
  res.setHeader('Permissions-Policy', 'camera=(self), microphone=(self)');
  next();
});

// Configuración ICE (STUN gratis + TURN opcional por variables de entorno)
app.get('/config', (req, res) => {
  const iceServers = [{ urls: 'stun:stun.l.google.com:19302' }];
  if (process.env.TURN_URL) {
    iceServers.push({
      urls: process.env.TURN_URL.split(','),
      username: process.env.TURN_USER,
      credential: process.env.TURN_PASS
    });
  }
  res.json({ iceServers });
});

app.use(express.static('public'));

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

let waiting = null;                  // usuario esperando pareja
const reportsByIp = new Map();       // ip -> cantidad de reportes recibidos
const bannedUntil = new Map();       // ip -> timestamp fin de bloqueo
const REPORTS_TO_BAN = 3;
const BAN_MS = 60 * 60 * 1000;       // 1 hora

const send = (ws, obj) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj)); };

function broadcastOnline() {
  const n = wss.clients.size;
  for (const c of wss.clients) send(c, { type: 'online', count: n });
}

function leave(ws) {
  if (waiting === ws) waiting = null;
  const p = ws.partner;
  if (p) {
    p.partner = null;
    send(p, { type: 'partner-left' });
  }
  ws.partner = null;
}

function join(ws) {
  leave(ws);
  if (waiting && waiting !== ws && waiting.readyState === 1) {
    const other = waiting;
    waiting = null;
    ws.partner = other;
    other.partner = ws;
    send(other, { type: 'matched', initiator: true });
    send(ws, { type: 'matched', initiator: false });
  } else {
    waiting = ws;
    send(ws, { type: 'waiting' });
  }
}

wss.on('connection', (ws, req) => {
  const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
  const until = bannedUntil.get(ip);
  if (until && until > Date.now()) {
    send(ws, { type: 'banned' });
    return ws.close();
  }
  ws.ip = ip;
  ws.partner = null;
  ws.msgTimes = [];
  broadcastOnline();

  ws.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw); } catch { return; }

    switch (m.type) {
      case 'join':
        join(ws);
        break;
      case 'next':
        leave(ws);
        join(ws);
        break;
      case 'stop':
        leave(ws);
        break;
      case 'signal':   // offer / answer / ice
        if (ws.partner) send(ws.partner, { type: 'signal', data: m.data });
        break;
      case 'chat': {
        const now = Date.now();
        ws.msgTimes = ws.msgTimes.filter(t => now - t < 3000);
        if (ws.msgTimes.length >= 5) return;           // límite de mensajes
        ws.msgTimes.push(now);
        const text = String(m.text || '').slice(0, 500);
        if (text && ws.partner) send(ws.partner, { type: 'chat', text });
        break;
      }
      case 'report': {
        const p = ws.partner;
        if (!p) return;
        const count = (reportsByIp.get(p.ip) || 0) + 1;
        reportsByIp.set(p.ip, count);
        if (count >= REPORTS_TO_BAN) {
          bannedUntil.set(p.ip, Date.now() + BAN_MS);
          reportsByIp.delete(p.ip);
          send(p, { type: 'banned' });
          p.close();
        }
        break;
      }
    }
  });

  ws.on('close', () => { leave(ws); broadcastOnline(); });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`MundoChat en http://localhost:${PORT}`));
