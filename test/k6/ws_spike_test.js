import { check } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
import { authToken as signedToken } from './lib/auth.js';
import { HOT_CHANNELS, PAYLOAD_SIZE, WS_URL } from './lib/config.js';
import { spikeScenarios } from './lib/scenarios.js';
import { delay, generatePayload } from './lib/util.js';
import { openWS, subscribe, unsubscribe, publish, close, extractChannelData } from './lib/ws.js';
import { metricValue, padLine, summaryArtifact } from './lib/summary.js';

const connectErrors = new Counter('connect_errors');
const streamErrors = new Counter('stream_errors');
const publishedMessages = new Counter('published_messages');
const receivedMessages = new Counter('received_messages');
const connectLatency = new Trend('connect_latency_ms', true);
const firstMessageLatency = new Trend('first_message_latency_ms', true);
const deliveryLatency = new Trend('delivery_latency_ms', true);
const connectionSuccess = new Rate('connection_success_rate');
const deliveryRate = new Rate('message_delivery_rate');

export const options = {
  tags: { transport: 'ws' },
  scenarios: spikeScenarios(),
  thresholds: {
    connect_errors: [{ threshold: 'count<100', abortOnFail: false }],
    connection_success_rate: [{ threshold: 'rate>0.90', abortOnFail: false }],
    connect_latency_ms: ['p(95)<3000'],
    first_message_latency_ms: ['p(95)<2000'],
    delivery_latency_ms: ['p(99)<1000'],
  },
};

async function connect(authToken, onMessage) {
  const t0 = Date.now();
  try {
    const ws = await openWS(authToken, {
      onMessage,
      onError() { streamErrors.add(1); },
    });
    connectLatency.add(Date.now() - t0);
    connectionSuccess.add(1);
    return ws;
  } catch (_) {
    connectErrors.add(1);
    connectionSuccess.add(0);
    return null;
  }
}

export async function subscriber() {
  let received = 0;
  let firstMsgRecorded = false;
  const subscribeTime = Date.now();

  const ws = await connect(signedToken(), (msg) => {
    if (msg.channel_message) {
      if (!firstMsgRecorded) {
        firstMsgRecorded = true;
        firstMessageLatency.add(Date.now() - subscribeTime);
      }
      received++;
      receivedMessages.add(1);
      const ts = parseInt((extractChannelData(msg) || '').split('|')[0], 10);
      if (ts > 0) deliveryLatency.add(Date.now() - ts);
    }
  });

  if (!ws) {
    check(null, { 'subscriber connected': () => false });
    await delay(2000);
    return;
  }

  const primary = HOT_CHANNELS[__VU % HOT_CHANNELS.length];
  subscribe(ws, primary, 'sub-primary');

  let secondary = null;
  if (__VU % 5 < 2) {
    secondary = HOT_CHANNELS[(__VU + 1) % HOT_CHANNELS.length];
    subscribe(ws, secondary, 'sub-secondary');
  }
  await delay(200);

  for (let i = 0; i < 30; i++) {
    await delay(5000);
    deliveryRate.add(received > 0 ? 1 : 0);
  }

  check(null, {
    'subscriber received messages': () => received > 0,
  });

  unsubscribe(ws, primary, 'unsub-primary');
  if (secondary) unsubscribe(ws, secondary, 'unsub-secondary');
  await delay(200);

  close(ws);
}

export async function publisher() {
  const ws = await connect(signedToken(), () => {});
  if (!ws) {
    check(null, { 'publisher connected': () => false });
    await delay(2000);
    return;
  }

  const target = HOT_CHANNELS[__VU % HOT_CHANNELS.length];
  subscribe(ws, target, 'sub-self');
  await delay(300);

  let sent = 0;
  for (let burst = 0; burst < 20; burst++) {
    for (let i = 0; i < 10; i++) {
      try {
        const payload = `${Date.now()}|${generatePayload(PAYLOAD_SIZE)}`;
        publish(ws, target, payload, `pub-${burst}-${i}`);
        publishedMessages.add(1);
        sent++;
      } catch (_) {
        streamErrors.add(1);
      }
    }
    await delay(500);
  }

  check(null, {
    'publisher sent all messages': () => sent === 200,
  });

  await delay(5000);

  unsubscribe(ws, target, 'unsub-self');
  await delay(200);
  close(ws);
}

export function handleSummary(data) {
  const lines = [
    '',
    '  SPIKE WS RESULTS',
    '  ' + '-'.repeat(40),
    padLine('Peak VUs', metricValue(data, 'vus_max', 'max')),
    padLine('Connect Errors', metricValue(data, 'connect_errors', 'count')),
    padLine('Stream Errors', metricValue(data, 'stream_errors', 'count')),
    padLine('Connection Success', metricValue(data, 'connection_success_rate', 'rate')),
    padLine('Connect Latency p95', metricValue(data, 'connect_latency_ms', 'p(95)') + 'ms'),
    '  ' + '-'.repeat(40),
    padLine('WS Endpoint', WS_URL),
    '  ' + '-'.repeat(40),
    padLine('Published Messages', metricValue(data, 'published_messages', 'count')),
    padLine('Received Messages', metricValue(data, 'received_messages', 'count')),
    padLine('First Msg Latency p95', metricValue(data, 'first_message_latency_ms', 'p(95)') + 'ms'),
    padLine('Delivery Latency p99', metricValue(data, 'delivery_latency_ms', 'p(99)') + 'ms'),
    padLine('Delivery Rate', metricValue(data, 'message_delivery_rate', 'rate')),
    '',
  ];

  console.log(lines.join('\n'));
  return summaryArtifact(data);
}
