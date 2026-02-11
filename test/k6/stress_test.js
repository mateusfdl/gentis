import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
import { PAYLOAD_SIZE } from './lib/config.js';
import { newClient, openStream, closeStream, generatePayload } from './lib/grpc.js';

const connectErrors = new Counter('connect_errors');
const publishedCount = new Counter('published_messages');
const receivedCount = new Counter('received_messages');
const publishLatency = new Trend('publish_latency', true);
const deliveryRate = new Rate('delivery_rate');

const client = newClient();

export const options = {
  stages: [
    { duration: '2m', target: 100 },
    { duration: '5m', target: 100 },
    { duration: '2m', target: 200 },
    { duration: '5m', target: 200 },
    { duration: '2m', target: 400 },
    { duration: '5m', target: 400 },
    { duration: '2m', target: 600 },
    { duration: '5m', target: 600 },
    { duration: '5m', target: 0 },
  ],
  thresholds: {
    connect_errors: ['count<50'],
    'publish_latency': ['p(95)<200'],
  },
};

export default function () {
  const channel = `stress-ch-${__VU % 20}`;
  let received = 0;
  let sent = 0;

  const conn = openStream(client, 'stress-test', {
    onData(msg) {
      if (msg.channelMessage) {
        received++;
        receivedCount.add(1);
      }
    },
    onError(err) {
      const s = String(err);
      if (!s.includes('canceled') && !s.includes('CANCELLED')) {
        connectErrors.add(1);
      }
    },
  });

  if (!conn) {
    connectErrors.add(1);
    check(null, { 'connected': () => false });
    return;
  }

  const extraChannels = __VU % 5 === 0 ? 2 : 0;
  conn.stream.write({ subscribe: { channel } });
  for (let i = 1; i <= extraChannels; i++) {
    conn.stream.write({ subscribe: { channel: `${channel}-extra-${i}` } });
  }
  sleep(0.3);

  const publishCount = 20;
  for (let i = 0; i < publishCount; i++) {
    const payload = generatePayload(PAYLOAD_SIZE);
    const t0 = Date.now();
    try {
      conn.stream.write({ publish: { channel, data: payload } });
      publishLatency.add(Date.now() - t0);
      publishedCount.add(1);
      sent++;
    } catch (_) {
      connectErrors.add(1);
    }
    sleep(0.05);
  }

  sleep(2);

  deliveryRate.add(received > 0 ? 1 : 0);

  check(null, {
    'published all messages': () => sent === publishCount,
    'received at least one message': () => received > 0,
  });

  conn.stream.write({ unsubscribe: { channel } });
  for (let i = 1; i <= extraChannels; i++) {
    conn.stream.write({ unsubscribe: { channel: `${channel}-extra-${i}` } });
  }
  sleep(0.2);

  closeStream(client, conn.stream);
}
