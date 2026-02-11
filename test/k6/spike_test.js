import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
import grpc from 'k6/net/grpc';
import { PAYLOAD_SIZE, PROTO_DIR, PROTO_FILE, SERVICE_METHOD } from './lib/config.js';
import { generatePayload } from './lib/grpc.js';

const SERVER_ADDR = __ENV.SERVER_ADDR || 'localhost:9000';
const RELAY_ADDR = __ENV.RELAY_ADDR || 'localhost:9001';

const connectErrors = new Counter('connect_errors');
const streamErrors = new Counter('stream_errors');
const publishedMessages = new Counter('published_messages');
const receivedMessages = new Counter('received_messages');
const connectLatency = new Trend('connect_latency_ms', true);
const firstMessageLatency = new Trend('first_message_latency_ms', true);
const connectionSuccess = new Rate('connection_success_rate');
const deliveryRate = new Rate('message_delivery_rate');

const HOT_CHANNELS = ['broadcast-1', 'broadcast-2', 'broadcast-3', 'broadcast-4', 'broadcast-5'];

const subscriberClient = new grpc.Client();
subscriberClient.load([PROTO_DIR], PROTO_FILE);

const publisherClient = new grpc.Client();
publisherClient.load([PROTO_DIR], PROTO_FILE);

export const options = {
  scenarios: {
    subscribers: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '30s', target: 800 },
        { duration: '30s', target: 1600 },
        { duration: '3m', target: 1600 },
        { duration: '30s', target: 0 },
      ],
      exec: 'subscriber',
      gracefulRampDown: '15s',
      gracefulStop: '15s',
    },
    publishers: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '30s', target: 200 },
        { duration: '30s', target: 400 },
        { duration: '2m', target: 400 },
        { duration: '30s', target: 0 },
      ],
      startTime: '1m',
      exec: 'publisher',
      gracefulRampDown: '15s',
      gracefulStop: '15s',
    },
  },
  thresholds: {
    connect_errors: [{ threshold: 'count<100', abortOnFail: false }],
    connection_success_rate: [{ threshold: 'rate>0.90', abortOnFail: false }],
    connect_latency_ms: ['p(95)<3000'],
    first_message_latency_ms: ['p(95)<2000'],
  },
};

function connect(client, addr, authToken) {
  const t0 = Date.now();

  try {
    client.connect(addr, { plaintext: true, timeout: '10s' });
  } catch (_) {
    connectErrors.add(1);
    connectionSuccess.add(0);
    return null;
  }

  let stream;
  try {
    stream = new grpc.Stream(client, SERVICE_METHOD);
  } catch (_) {
    streamErrors.add(1);
    client.close();
    return null;
  }

  stream.on('error', () => { streamErrors.add(1); });

  stream.write({ connect: { authToken } });
  sleep(0.1);

  connectLatency.add(Date.now() - t0);
  connectionSuccess.add(1);
  return { client, stream };
}

function teardown(client, stream) {
  try { stream.end(); } catch (_) {}
  try { client.close(); } catch (_) {}
}

export function subscriber() {
  const conn = connect(subscriberClient, RELAY_ADDR, 'spike-sub');
  if (!conn) {
    check(null, { 'subscriber connected to relay': () => false });
    sleep(2);
    return;
  }

  let received = 0;
  let firstMsgRecorded = false;
  const subscribeTime = Date.now();

  conn.stream.on('data', (msg) => {
    if (msg.channelMessage) {
      if (!firstMsgRecorded) {
        firstMsgRecorded = true;
        firstMessageLatency.add(Date.now() - subscribeTime);
      }
      received++;
      receivedMessages.add(1);
    }
  });

  const primary = HOT_CHANNELS[__VU % HOT_CHANNELS.length];
  conn.stream.write({ subscribe: { channel: primary } });

  let secondary = null;
  if (__VU % 5 < 2) {
    secondary = HOT_CHANNELS[(__VU + 1) % HOT_CHANNELS.length];
    conn.stream.write({ subscribe: { channel: secondary } });
  }
  sleep(0.2);

  for (let i = 0; i < 30; i++) {
    sleep(5);
    deliveryRate.add(received > 0 ? 1 : 0);
  }

  check(null, {
    'subscriber received messages': () => received > 0,
  });

  conn.stream.write({ unsubscribe: { channel: primary } });
  if (secondary) conn.stream.write({ unsubscribe: { channel: secondary } });
  sleep(0.2);

  teardown(subscriberClient, conn.stream);
}

export function publisher() {
  const conn = connect(publisherClient, SERVER_ADDR, 'spike-pub');
  if (!conn) {
    check(null, { 'publisher connected to server': () => false });
    sleep(2);
    return;
  }

  conn.stream.on('data', () => {});

  const target = HOT_CHANNELS[__VU % HOT_CHANNELS.length];
  conn.stream.write({ subscribe: { channel: target } });
  sleep(0.3);

  let sent = 0;
  for (let burst = 0; burst < 20; burst++) {
    for (let i = 0; i < 10; i++) {
      try {
        conn.stream.write({
          publish: { channel: target, data: generatePayload(PAYLOAD_SIZE) },
        });
        publishedMessages.add(1);
        sent++;
      } catch (_) {
        streamErrors.add(1);
      }
    }
    sleep(0.5);
  }

  check(null, {
    'publisher sent all messages': () => sent === 200,
  });

  sleep(5);

  conn.stream.write({ unsubscribe: { channel: target } });
  sleep(0.2);
  teardown(publisherClient, conn.stream);
}

export function handleSummary(data) {
  const val = (name, field) => {
    const m = data.metrics[name];
    if (!m) return 'N/A';
    if (field === 'count') return m.values.count || 0;
    if (field === 'rate') return (m.values.rate * 100).toFixed(1) + '%';
    return (m.values[field] || 0).toFixed(1);
  };

  const line = (label, value) =>
    `  ${label.padEnd(26)} ${String(value).padStart(12)}`;

  const lines = [
    '',
    '  SPIKE RELAY FAN-OUT RESULTS',
    '  ' + '-'.repeat(40),
    line('Peak VUs', val('vus_max', 'max')),
    line('Connect Errors', val('connect_errors', 'count')),
    line('Stream Errors', val('stream_errors', 'count')),
    line('Connection Success', val('connection_success_rate', 'rate')),
    line('Connect Latency p95', val('connect_latency_ms', 'p(95)') + 'ms'),
    '  ' + '-'.repeat(40),
    line('Publishers → Server', SERVER_ADDR),
    line('Subscribers → Relay', RELAY_ADDR),
    '  ' + '-'.repeat(40),
    line('Published Messages', val('published_messages', 'count')),
    line('Received Messages', val('received_messages', 'count')),
    line('First Msg Latency p95', val('first_message_latency_ms', 'p(95)') + 'ms'),
    line('Delivery Rate', val('message_delivery_rate', 'rate')),
    '',
  ];

  console.log(lines.join('\n'));
  return {};
}
