import { authToken } from './lib/auth.js';
import { check } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
import { PAYLOAD_SIZE } from './lib/config.js';
import { stressStages } from './lib/scenarios.js';
import { delay, generatePayload } from './lib/util.js';
import { openWS, subscribe, unsubscribe, publish, close, extractChannelData } from './lib/ws.js';

const connectErrors = new Counter('connect_errors');
const publishedCount = new Counter('published_messages');
const receivedCount = new Counter('received_messages');
const deliveryLatency = new Trend('delivery_latency_ms', true);
const deliveryRate = new Rate('delivery_rate');

export const options = {
  tags: { transport: 'ws' },
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

  let ws;
  try {
    ws = await openWS(authToken(), {
      onMessage(msg) {
        if (msg.channel_message) {
          received++;
          receivedCount.add(1);
          const ts = parseInt((extractChannelData(msg) || '').split('|')[0], 10);
          if (ts > 0) deliveryLatency.add(Date.now() - ts);
        }
      },
      onError() {
        connectErrors.add(1);
      },
    });
  } catch (_) {
    connectErrors.add(1);
    check(null, { 'connected': () => false });
    return;
  }

  const extraChannels = __VU % 5 === 0 ? 2 : 0;
  subscribe(ws, channel, 'sub-primary');
  for (let i = 1; i <= extraChannels; i++) {
    subscribe(ws, `${channel}-extra-${i}`, `sub-extra-${i}`);
  }
  await delay(300);

  const publishCount = 20;
  for (let i = 0; i < publishCount; i++) {
    const payload = `${Date.now()}|${generatePayload(PAYLOAD_SIZE)}`;
    try {
      publish(ws, channel, payload, `pub-${i}`);
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

  unsubscribe(ws, channel, 'unsub-primary');
  for (let i = 1; i <= extraChannels; i++) {
    unsubscribe(ws, `${channel}-extra-${i}`, `unsub-extra-${i}`);
  }
  await delay(200);

  close(ws);
}
