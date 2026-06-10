import { authToken } from './lib/auth.js';
import { check } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
import { PAYLOAD_SIZE } from './lib/config.js';
import { stressStages } from './lib/scenarios.js';
import { delay, generatePayload } from './lib/util.js';
import { newClient, openStream, closeStream, subscribe, unsubscribe, publish, channelData } from './lib/grpc.js';

const connectErrors = new Counter('connect_errors');
const publishedCount = new Counter('published_messages');
const receivedCount = new Counter('received_messages');
const deliveryLatency = new Trend('delivery_latency_ms', true);
const deliveryRate = new Rate('delivery_rate');

const client = newClient();

export const options = {
  stages: stressStages,
  thresholds: {
    connect_errors: ['count<50'],
    delivery_latency_ms: ['p(99)<1000'],
  },
};

export default async function () {
  const channel = `stress-ch-${__VU % 20}`;
  let received = 0;
  let sent = 0;

  const conn = openStream(client, authToken(), {
    onData(msg) {
      if (msg.channelMessage) {
        received++;
        receivedCount.add(1);
        const ts = parseInt((channelData(msg) || '').split('|')[0], 10);
        if (ts > 0) deliveryLatency.add(Date.now() - ts);
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
  subscribe(conn.stream, channel);
  for (let i = 1; i <= extraChannels; i++) {
    subscribe(conn.stream, `${channel}-extra-${i}`);
  }
  await delay(300);

  const publishCount = 20;
  for (let i = 0; i < publishCount; i++) {
    const payload = `${Date.now()}|${generatePayload(PAYLOAD_SIZE)}`;
    try {
      publish(conn.stream, channel, payload);
      publishedCount.add(1);
      sent++;
    } catch (_) {
      connectErrors.add(1);
    }
    await delay(50);
  }

  await delay(2000);

  deliveryRate.add(received > 0 ? 1 : 0);

  check(null, {
    'published all messages': () => sent === publishCount,
    'received at least one message': () => received > 0,
  });

  unsubscribe(conn.stream, channel);
  for (let i = 1; i <= extraChannels; i++) {
    unsubscribe(conn.stream, `${channel}-extra-${i}`);
  }
  await delay(200);

  closeStream(client, conn.stream);
}
