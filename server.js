// MundoChat - servidor de señalización, emparejamiento y protección (prototipo)
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

// Los archivos del sitio pueden estar en la raíz o dentro de public/.
// Si existen en la raíz (versión más reciente subida a mano), se usan primero.
const path = require('path');
const fs = require('fs');
const SITE_FILES = ['index.html', 'terminos.html', 'privacidad.html', 'robots.txt', 'sitemap.xml'];
const findFile = (name) => {
  const inRoot = path.join(__dirname, name);
  if (fs.existsSync(inRoot)) return inRoot;
  const inPublic = path.join(__dirname, 'public', name);
  return fs.existsSync(inPublic) ? inPublic : null;
};
app.get('/', (req, res) => res.sendFile(findFile('index.html')));
SITE_FILES.forEach((name) => {
  app.get('/' + name, (req, res) => {
    const f = findFile(name);
    if (f) res.sendFile(f); else res.status(404).end();
  });
});

app.use(express.static('public'));

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// Una cola de espera por modo: 'video' y 'text'
const waiting = { video: null, text: null };
const reportsByIp = new Map();   // ip -> reportes recibidos
const bannedUntil = new Map();   // ip -> fin del bloqueo
const REPORTS_TO_BAN = 3;
const BAN_REPORTS_MS = 60 * 60 * 1000;   // 1 hora por reportes
const STRIKES_TO_BAN = 5;
const BAN_STRIKES_MS = 10 * 60 * 1000;   // 10 min por mensajes bloqueados

const send = (ws, obj) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj)); };

// ---------- Filtro de mensajes ----------
const normalize = (t) => t.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '');
const BAD_WORDS = [
  'nude', 'nudes', 'desnud', 'porn', 'sexting', 'onlyfans', 'sexcam', 'webcam sex',
  'tetas', 'verga', 'pene', 'follar', 'mamada', 'putita', 'hentai',
  'whatsapp', 'telegram', 'snapchat', 'instagram', 'discord', 'cashapp', 'paypal'
];
function checkMessage(raw) {
  const t = normalize(raw);
  const compact = t.replace(/[\s.\-()_]/g, '');
  if (/https?:|www\.|[a-z0-9-]+\.(com|net|org|io|me|co|ly|xyz|ru|cc|app|tv)\b/.test(t)) return 'No se permiten enlaces.';
  if (/@/.test(t)) return 'No se permiten correos ni usuarios de redes.';
  if (/\d{7,}/.test(compact)) return 'No se permite compartir números de teléfono.';
  if (BAD_WORDS.some(w => t.includes(w))) return 'Ese mensaje rompe las normas de la comunidad.';
  return null;
}

// ---------- Emparejamiento ----------
function broadcastOnline() {
  const n = wss.clients.size;
  for (const c of wss.clients) send(c, { type: 'online', count: n });
}

function leave(ws) {
  for (const k of Object.keys(waiting)) if (waiting[k] === ws) waiting[k] = null;
  const p = ws.partner;
  if (p) {
    p.partner = null;
    send(p, { type: 'partner-left' });
  }
  ws.partner = null;
}

function join(ws, mode) {
  leave(ws);
  ws.mode = mode === 'text' ? 'text' : 'video';
  const other = waiting[ws.mode];
  if (other && other !== ws && other.readyState === 1) {
    waiting[ws.mode] = null;
    ws.partner = other;
    other.partner = ws;
    send(other, { type: 'matched', initiator: true, mode: ws.mode });
    send(ws, { type: 'matched', initiator: false, mode: ws.mode });
  } else {
    waiting[ws.mode] = ws;
    send(ws, { type: 'waiting', mode: ws.mode });
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
  ws.mode = 'video';
  ws.msgTimes = [];
  ws.strikes = 0;
  broadcastOnline();

  ws.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw); } catch { return; }

    switch (m.type) {
      case 'join':
        join(ws, m.mode);
        break;
      case 'next':
        join(ws, m.mode || ws.mode);
        break;
      case 'stop':
        leave(ws);
        break;
      case 'signal':
        if (ws.partner && ws.mode === 'video') send(ws.partner, { type: 'signal', data: m.data });
        break;
      case 'chat': {
        if (!ws.partner) return;
        const now = Date.now();
        ws.msgTimes = ws.msgTimes.filter(t => now - t < 3000);
        if (ws.msgTimes.length >= 5) return;
        ws.msgTimes.push(now);
        const text = String(m.text || '').trim().slice(0, 500);
        if (!text) return;
        const problem = checkMessage(text);
        if (problem) {
          ws.strikes++;
          send(ws, { type: 'blocked', reason: problem });
          if (ws.strikes >= STRIKES_TO_BAN) {
            bannedUntil.set(ws.ip, Date.now() + BAN_STRIKES_MS);
            send(ws, { type: 'banned' });
            ws.close();
          }
          return;
        }
        send(ws.partner, { type: 'chat', text });
        send(ws, { type: 'sent', text });
        break;
      }
      case 'report': {
        const p = ws.partner;
        if (!p) return;
        const count = (reportsByIp.get(p.ip) || 0) + 1;
        reportsByIp.set(p.ip, count);
        if (count >= REPORTS_TO_BAN) {
          bannedUntil.set(p.ip, Date.now() + BAN_REPORTS_MS);
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
