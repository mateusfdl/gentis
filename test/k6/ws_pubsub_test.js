import { Counter, Rate, Trend } from 'k6/metrics';
import { randomIntBetween } from 'https://jslib.k6.io/k6-utils/1.2.0/index.js';
import { CHANNEL_PREFIX, PAYLOAD_SIZE, WS_AUTH_TOKEN } from './lib/config.js';
import { pubsubScenarios } from './lib/scenarios.js';
import { delay, generatePayload } from './lib/util.js';
import { openWS, subscribe, unsubscribe, publish, close, extractChannelData } from './lib/ws.js';

const subscribeLatency = new Trend('subscribe_latency', true);
const messageLatency = new Trend('message_latency', true);
const messagesReceived = new Counter('messages_received');
const connectionErrors = new Counter('connection_errors');
const subscribeSuccess = new Rate('subscribe_success_rate');
const publishSuccess = new Rate('publish_success_rate');

export const options = {
  tags: { transport: 'ws' },
  scenarios: pubsubScenarios(),
  thresholds: {
    subscribe_latency: ['p(95)<100'],
    message_latency: ['p(95)<1000'],
    subscribe_success_rate: ['rate>0.95'],
    publish_success_rate: ['rate>0.95'],
    connection_errors: ['count<100'],
  },
};

export default async function () {
  const channel = `${CHANNEL_PREFIX}-${__VU % 10}`;
  let subscribeStart = 0;
  let subscribedOk = false;

  let ws;
  try {
    ws = await openWS(WS_AUTH_TOKEN, {
      onMessage(msg) {
        if (msg.subscribed && msg.subscribed.channel === channel) {
          subscribedOk = true;
          if (subscribeStart) subscribeLatency.add(Date.now() - subscribeStart);
        }
        if (msg.channel_message) {
          messagesReceived.add(1);
          const ts = parseInt((extractChannelData(msg) || '').split('|')[0], 10);
          if (ts > 0) messageLatency.add(Date.now() - ts);
        }
      },
      onError() {
        connectionErrors.add(1);
      },
    });
  } catch (_) {
    connectionErrors.add(1);
    subscribeSuccess.add(0);
    publishSuccess.add(0);
    return;
  }

  await delay(randomIntBetween(100, 300));

  subscribeStart = Date.now();
  subscribe(ws, channel, 'sub');
  await delay(300);
  subscribeSuccess.add(subscribedOk ? 1 : 0);

  await delay(randomIntBetween(100, 200));

  const count = randomIntBetween(3, 8);
  for (let i = 0; i < count; i++) {
    const body = `${Date.now()}|${generatePayload(PAYLOAD_SIZE)}`;
    try {
      publish(ws, channel, body, `pub-${i}`);
      publishSuccess.add(1);
    } catch (_) {
      publishSuccess.add(0);
    }
    await delay(randomIntBetween(50, 150));
  }

  await delay(randomIntBetween(2000, 5000));

  unsubscribe(ws, channel, 'unsub');
  await delay(300);

  close(ws);
  await delay(randomIntBetween(100, 300));
}
