import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
import { randomIntBetween } from 'https://jslib.k6.io/k6-utils/1.2.0/index.js';
import { newClient, openStream, closeStream } from './lib/grpc.js';

const connectionUptime = new Trend('connection_uptime_seconds', true);
const reconnects = new Counter('reconnect_count');
const messagesSent = new Counter('messages_sent');
const messagesReceived = new Counter('messages_received');
const deliverySuccess = new Rate('delivery_success');

const SOAK_DURATION = __ENV.SOAK_DURATION || '30m';
const ACTIVITY_INTERVAL = parseInt(__ENV.SOAK_INTERVAL || '10');

const client = newClient();

function durationToSeconds(d) {
  const m = d.match(/(\d+)([smh])/);
  if (!m) return 1800;
  const v = parseInt(m[1]);
  switch (m[2]) {
    case 's': return v;
    case 'm': return v * 60;
    case 'h': return v * 3600;
    default: return 1800;
  }
}

export const options = {
  scenarios: {
    soak: {
      executor: 'constant-vus',
      vus: parseInt(__ENV.SOAK_VUS || '50'),
      duration: SOAK_DURATION,
    },
  },
  thresholds: {
    reconnect_count: ['count<20'],
    delivery_success: ['rate>0.80'],
  },
};

export default function () {
  const channel = `soak-ch-${__VU % 5}`;
  const start = Date.now();
  let sent = 0;
  let received = 0;

  const conn = openStream(client, 'soak-test', {
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
    sleep(5);
    return;
  }

  conn.stream.write({ subscribe: { channel } });
  sleep(0.5);

  const totalSec = durationToSeconds(SOAK_DURATION);
  const iterations = Math.floor(totalSec / ACTIVITY_INTERVAL);

  for (let i = 0; i < iterations; i++) {
    try {
      conn.stream.write({
        publish: { channel, data: `soak-${__VU}-${i}-${Date.now()}` },
      });
      sent++;
      messagesSent.add(1);
    } catch (_) {
      reconnects.add(1);
      break;
    }

    if (i % 30 === 0 && __VU % 3 === 0) {
      const tmp = `soak-tmp-${__VU}-${i}`;
      conn.stream.write({ subscribe: { channel: tmp } });
      sleep(1);
      conn.stream.write({ unsubscribe: { channel: tmp } });
    }

    if (i % 10 === 0) {
      conn.stream.write({ ping: {} });
    }

    sleep(ACTIVITY_INTERVAL);
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

  conn.stream.write({ unsubscribe: { channel } });
  sleep(0.2);
  closeStream(client, conn.stream);
}
