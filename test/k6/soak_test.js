import { authToken } from './lib/auth.js';
import { check } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
import { newClient, openStream, closeStream, subscribe, unsubscribe, publish, ping } from './lib/grpc.js';
import { delay, durationToSeconds } from './lib/util.js';

const connectionUptime = new Trend('connection_uptime_seconds', true);
const reconnects = new Counter('reconnect_count');
const messagesSent = new Counter('messages_sent');
const messagesReceived = new Counter('messages_received');
const deliverySuccess = new Rate('delivery_success');

const SOAK_DURATION = __ENV.SOAK_DURATION || '30m';
const ACTIVITY_INTERVAL = parseInt(__ENV.SOAK_INTERVAL || '10', 10);

const client = newClient();

export const options = {
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

  const conn = openStream(client, authToken(), {
    onData(msg) {
      if (msg.channelMessage) received++;
    },
    onError() {
      reconnects.add(1);
    },
  });

  if (!conn) {
    reconnects.add(1);
    check(null, { 'connected': () => false });
    await delay(5000);
    return;
  }

  subscribe(conn.stream, channel);
  await delay(500);

  const totalSec = durationToSeconds(SOAK_DURATION);
  const iterations = Math.floor(totalSec / ACTIVITY_INTERVAL);

  for (let i = 0; i < iterations; i++) {
    try {
      publish(conn.stream, channel, `soak-${__VU}-${i}-${Date.now()}`);
      sent++;
      messagesSent.add(1);
    } catch (_) {
      reconnects.add(1);
      break;
    }

    if (i % 30 === 0 && __VU % 3 === 0) {
      const tmp = `soak-tmp-${__VU}-${i}`;
      subscribe(conn.stream, tmp);
      await delay(1000);
      unsubscribe(conn.stream, tmp);
    }

    if (i % 10 === 0) {
      ping(conn.stream);
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

  unsubscribe(conn.stream, channel);
  await delay(200);
  closeStream(client, conn.stream);
}
