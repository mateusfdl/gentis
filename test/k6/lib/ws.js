import { WebSocket } from 'k6/websockets';
import { setTimeout, clearTimeout } from 'k6/timers';
import { WS_URL } from './config.js';

export function openWS(authToken, opts = {}) {
  const {
    onMessage,
    onError,
    onClose,
    connectTimeoutMs = 5000,
    url = WS_URL,
  } = opts;

  return new Promise((resolve, reject) => {
    let settled = false;
    let timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      try { ws.close(); } catch (_) {}
      reject(new Error(`ws connect timeout after ${connectTimeoutMs}ms`));
    }, connectTimeoutMs);

    const ws = new WebSocket(url);

    ws.addEventListener('open', () => {
      try {
        ws.send(JSON.stringify({ id: 'c0', connect: { auth_token: authToken } }));
      } catch (e) {
        if (!settled) {
          settled = true;
          clearTimeout(timer);
          reject(e);
        }
      }
    });

    ws.addEventListener('message', (e) => {
      let msg;
      try { msg = JSON.parse(e.data); } catch (_) { return; }
      if (!settled && msg && msg.connected) {
        settled = true;
        clearTimeout(timer);
        resolve(ws);
      }
      if (onMessage) {
        try { onMessage(msg); } catch (_) {}
      }
    });

    ws.addEventListener('error', (err) => {
      if (!settled) {
        settled = true;
        clearTimeout(timer);
        reject(err);
        return;
      }
      if (onError) onError(err);
    });

    ws.addEventListener('close', (evt) => {
      if (!settled) {
        settled = true;
        clearTimeout(timer);
        reject(new Error('ws closed before connected'));
        return;
      }
      if (onClose) onClose(evt);
    });
  });
}

export function send(ws, msg) {
  ws.send(JSON.stringify(msg));
}

export function subscribe(ws, channel, id = '') {
  send(ws, { id, subscribe: { channel } });
}

export function unsubscribe(ws, channel, id = '') {
  send(ws, { id, unsubscribe: { channel } });
}

export function publish(ws, channel, body, id = '') {
  const data = JSON.stringify(body);
  ws.send(`{"id":${JSON.stringify(id)},"publish":{"channel":${JSON.stringify(channel)},"data":${data}}}`);
}

export function ping(ws, id = '') {
  send(ws, { id, ping: {} });
}

export function close(ws) {
  try { ws.close(); } catch (_) {}
}

export function extractChannelData(msg) {
  if (!msg || !msg.channel_message) return null;
  const d = msg.channel_message.data;
  if (typeof d === 'string') return d;
  if (d == null) return null;
  return JSON.stringify(d);
}
