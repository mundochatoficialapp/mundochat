
// MundoChat - servidor de señalización, emparejamiento y protección
const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const path = require('path');
const fs = require('fs');

const app = express();
app.set('trust proxy', true);

app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
  res.setHeader('Permissions-Policy', 'camera=(self), microphone=(self)');
  next();
});

// Configuración ICE
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

// Archivos del sitio: primero raíz, después public/
const SITE_FILES = [
  'index.html',
  'terminos.html',
  'privacidad.html',
  'robots.txt',
  'sitemap.xml'
];

const findFile = (name) => {
  const inRoot = path.join(__dirname, name);
  if (fs.existsSync(inRoot)) return inRoot;

  const inPublic = path.join(__dirname, 'public', name);
  return fs.existsSync(inPublic) ? inPublic : null;
};

app.get('/', (req, res) => {
  const file = findFile('index.html');
  if (file) return res.sendFile(file);
  res.status(404).send('No se encontró index.html');
});

SITE_FILES.forEach((name) => {
  app.get('/' + name, (req, res) => {
    const file = findFile(name);
    if (file) return res.sendFile(file);
    res.status(404).end();
  });
});

app.get(/^\/(google[0-9a-f]+\.html)$/, (req, res) => {
  const file = findFile(req.params[0]);
  if (file) return res.sendFile(file);
  res.status(404).end();
});

app.use(express.static(path.join(__dirname, 'public')));

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// Una cola por combinación de modalidad y ubicación.
// Ejemplos:
// video|random
// video|country|colombia
// video|city|colombia|cartagena
// text|country|colombia

const waiting = new Map();

const reportsByIp = new Map();
const bannedUntil = new Map();

const REPORTS_TO_BAN = 3;
const BAN_REPORTS_MS = 60 * 60 * 1000;
const STRIKES_TO_BAN = 5;
const BAN_STRIKES_MS = 10 * 60 * 1000;

const send = (ws, obj) => {
  if (ws && ws.readyState === 1) {
    ws.send(JSON.stringify(obj));
  }
};

// ---------- Normalización de ubicación ----------

function normalizeLocationValue(value, maxLength = 60) {
  return String(value || '')
    .trim()
    .slice(0, maxLength)
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9\s'-]/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function parseLocation(raw) {
  const scope = ['random', 'country', 'city'].includes(raw?.scope)
    ? raw.scope
    : 'random';

  if (scope === 'random') {
    return { scope: 'random', country: '', city: '' };
  }

  const country = normalizeLocationValue(raw?.country);

  if (!country) return null;

  if (scope === 'country') {
    return { scope, country, city: '' };
  }

  const city = normalizeLocationValue(raw?.city);

  if (!city) return null;

  return { scope, country, city };
}

function getQueueKey(mode, location) {
  if (location.scope === 'random') {
    return `${mode}|random`;
  }

  if (location.scope === 'country') {
    return `${mode}|country|${location.country}`;
  }

  return `${mode}|city|${location.country}|${location.city}`;
}

// ---------- Filtro de mensajes ----------

const normalize = (t) =>
  t.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '');

const BAD_WORDS = [
  'nude', 'nudes', 'desnud', 'porn', 'sexting', 'onlyfans',
  'sexcam', 'webcam sex', 'tetas', 'verga', 'pene', 'follar',
  'mamada', 'putita', 'hentai', 'whatsapp', 'telegram',
  'snapchat', 'instagram', 'discord', 'cashapp', 'paypal'
];

function checkMessage(raw) {
  const t = normalize(raw);
  const compact = t.replace(/[\s.\-()_]/g, '');

  if (/https?:|www\.|[a-z0-9-]+\.(com|net|org|io|me|co|ly|xyz|ru|cc|app|tv)\b/.test(t)) {
    return 'No se permiten enlaces.';
  }

  if (/@/.test(t)) return 'No se permiten correos ni usuarios de redes.';
  if (/\d{7,}/.test(compact)) return 'No se permite compartir números de teléfono.';

  if (BAD_WORDS.some(w => t.includes(w))) {
    return 'Ese mensaje rompe las normas de la comunidad.';
  }

  return null;
}

// ---------- Usuarios conectados ----------

function broadcastOnline() {
  const count = wss.clients.size;

  for (const client of wss.clients) {
    send(client, { type: 'online', count });
  }
}

function leave(ws) {
  // Retirar al usuario de cualquier cola donde esté esperando.
  for (const [key, user] of waiting.entries()) {
    if (user === ws) waiting.delete(key);
  }

  const partner = ws.partner;

  if (partner) {
    partner.partner = null;
    send(partner, { type: 'partner-left' });
  }

  ws.partner = null;
  ws.queueKey = null;
}

// ---------- Emparejamiento por ubicación ----------

function join(ws, requestedMode, rawLocation) {
  leave(ws);

  const mode = requestedMode === 'text' ? 'text' : 'video';
  const location = parseLocation(rawLocation);

  if (!location) {
    send(ws, {
      type: 'location-error',
      message: 'Selecciona un país y una ciudad válida para continuar.'
    });
    return;
  }

  const queueKey = getQueueKey(mode, location);

  ws.mode = mode;
  ws.location = location;
  ws.queueKey = queueKey;

  let other = waiting.get(queueKey);

  // Limpiar una conexión que ya no esté disponible.
  if (other && (other.readyState !== 1 || other === ws || other.partner)) {
    waiting.delete(queueKey);
    other = null;
  }

  if (other) {
    waiting.delete(queueKey);

    ws.partner = other;
    other.partner = ws;

    ws.queueKey = null;
    other.queueKey = null;

    send(other, {
      type: 'matched',
      initiator: true,
      mode,
      location
    });

    send(ws, {
      type: 'matched',
      initiator: false,
      mode,
      location
    });

  } else {
    waiting.set(queueKey, ws);

    send(ws, {
      type: 'waiting',
      mode,
      location
    });
  }
}

// ---------- Conexiones WebSocket ----------

wss.on('connection', (ws, req) => {
  const ip = (
    req.headers['x-forwarded-for'] ||
    req.socket.remoteAddress ||
    ''
  ).split(',')[0].trim();

  const until = bannedUntil.get(ip);

  if (until && until > Date.now()) {
    send(ws, { type: 'banned' });
    return ws.close();
  }

  ws.ip = ip;
  ws.partner = null;
  ws.mode = 'video';
  ws.location = { scope: 'random', country: '', city: '' };
  ws.queueKey = null;
  ws.msgTimes = [];
  ws.strikes = 0;

  broadcastOnline();

  ws.on('message', (raw) => {
    let message;

    try {
      message = JSON.parse(raw);
    } catch {
      return;
    }

    switch (message.type) {
      case 'join':
        join(ws, message.mode, message.location);
        break;

      case 'next':
        join(
          ws,
          message.mode || ws.mode,
          message.location || ws.location
        );
        break;

      case 'stop':
        leave(ws);
        break;

      case 'signal':
        if (ws.partner && ws.mode === 'video') {
          send(ws.partner, {
            type: 'signal',
            data: message.data
          });
        }
        break;

      case 'chat': {
        if (!ws.partner) return;

        const now = Date.now();

        ws.msgTimes = ws.msgTimes.filter(t => now - t < 3000);

        if (ws.msgTimes.length >= 5) return;

        ws.msgTimes.push(now);

        const text = String(message.text || '').trim().slice(0, 500);

        if (!text) return;

        const problem = checkMessage(text);

        if (problem) {
          ws.strikes++;

          send(ws, {
            type: 'blocked',
            reason: problem
          });

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
        const partner = ws.partner;

        if (!partner) return;

        const count = (reportsByIp.get(partner.ip) || 0) + 1;
        reportsByIp.set(partner.ip, count);

        if (count >= REPORTS_TO_BAN) {
          bannedUntil.set(partner.ip, Date.now() + BAN_REPORTS_MS);
          reportsByIp.delete(partner.ip);

          send(partner, { type: 'banned' });
          partner.close();
        }

        break;
      }
    }
  });

  ws.on('close', () => {
    leave(ws);
    broadcastOnline();
  });

  ws.on('error', () => {
    leave(ws);
  });
});

const PORT = process.env.PORT || 3000;

server.listen(PORT, () => {
  console.log(`MundoChat en http://localhost:${PORT}`);
});
