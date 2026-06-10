import { authToken } from './lib/auth.js';
import { check } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
import { delay, durationToSeconds } from './lib/util.js';
import { openWS, subscribe, unsubscribe, publish, ping, close } from './lib/ws.js';

const connectionUptime = new Trend('connection_uptime_seconds', true);
const reconnects = new Counter('reconnect_count');
const messagesSent = new Counter('messages_sent');
const messagesReceived = new Counter('messages_received');
const deliverySuccess = new Rate('delivery_success');

const SOAK_DURATION = __ENV.SOAK_DURATION || '30m';
const ACTIVITY_INTERVAL = parseInt(__ENV.SOAK_INTERVAL || '10', 10);

export const options = {
  tags: { transport: 'ws' },
  scenarios: {
    soak: {
      executor: 'constant-vus',
      vus: parseInt(__ENV.SOAK_VUS || '50', 10),
      duration: SOAK_DURATION,
    },
  },
  thresholds: {
    reconnect_count: ['count<20'],
    delivery_success: ['rate>0.80'],
  },
};

export default async function () {
  const channel = `soak-ch-${__VU % 5}`;
  const start = Date.now();
  let sent = 0;
  let received = 0;

  let ws;
  try {
    ws = await openWS(authToken(), {
      onMessage(msg) { if (msg.channel_message) received++; },
      onError() { reconnects.add(1); },
    });
  } catch (_) {
    reconnects.add(1);
    check(null, { 'connected': () => false });
    await delay(5000);
    return;
  }

  subscribe(ws, channel, 'sub');
  await delay(500);

  const totalSec = durationToSeconds(SOAK_DURATION);
  const iterations = Math.floor(totalSec / ACTIVITY_INTERVAL);

  for (let i = 0; i < iterations; i++) {
    try {
      publish(ws, channel, `soak-${__VU}-${i}-${Date.now()}`, `pub-${i}`);
      sent++;
      messagesSent.add(1);
    } catch (_) {
      reconnects.add(1);
      break;
    }

    if (i % 30 === 0 && __VU % 3 === 0) {
      const tmp = `soak-tmp-${__VU}-${i}`;
      subscribe(ws, tmp, `sub-tmp-${i}`);
      await delay(1000);
      unsubscribe(ws, tmp, `unsub-tmp-${i}`);
    }

    if (i % 10 === 0) {
      ping(ws, `ping-${i}`);
    }

    await delay(ACTIVITY_INTERVAL * 1000);
  }

  const uptimeSec = (Date.now() - start) / 1000;
  connectionUptime.add(uptimeSec);
  messagesReceived.add(received);

  for (let i = 0; i < sent; i++) {
    deliverySuccess.add(i < received ? 1 : 0);
  }

  check(null, {
    'connection stayed alive': () => uptimeSec > totalSec * 0.8,
    'received messages': () => received > 0,
  });

  unsubscribe(ws, channel, 'unsub');
  await delay(200);
  close(ws);
}
