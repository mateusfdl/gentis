import grpc from 'k6/net/grpc';
import { sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

// Spike test - fan-in/fan-out messaging with 2k connections
//
// Topology:
//   - 5 "hot" channels where most subscribers concentrate (fan-out)
//   - ~80% of VUs are subscribers (listen on 1-2 hot channels)
//   - ~20% of VUs are publishers (blast messages into hot channels = fan-in)
//   - Each publish on a hot channel fans out to hundreds of subscribers

const connectErrors = new Counter('connect_errors');
const streamErrors = new Counter('stream_errors');
const publishedMessages = new Counter('published_messages');
const receivedMessages = new Counter('received_messages');
const connectLatency = new Trend('connect_latency_ms');
const firstMessageLatency = new Trend('first_message_latency_ms');
const connectionSuccess = new Rate('connection_success_rate');
const deliveryRate = new Rate('message_delivery_rate');

const SERVER_ADDR = __ENV.SERVER_ADDR || 'localhost:9000';
const RELAY_ADDR = __ENV.RELAY_ADDR || 'localhost:9001';
const TARGET = __ENV.TARGET || 'server';
const PAYLOAD_SIZE = parseInt(__ENV.PAYLOAD_SIZE || '256');

const ADDR = TARGET === 'relay' ? RELAY_ADDR : SERVER_ADDR;

// 5 hot channels that all subscribers will join
const HOT_CHANNELS = ['broadcast-1', 'broadcast-2', 'broadcast-3', 'broadcast-4', 'broadcast-5'];

const client = new grpc.Client();
client.load(['../../api/proto'], 'gentis/v1/gentis.proto');

export const options = {
  scenarios: {
    // Subscribers: 1600 VUs ramp up, connect and hold open
    subscribers: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '30s', target: 800 },    // First wave
        { duration: '30s', target: 1600 },   // Full subscriber pool
        { duration: '3m', target: 1600 },    // Hold — receive fan-out messages
        { duration: '30s', target: 0 },      // Drain
      ],
      exec: 'subscriber',
      gracefulRampDown: '15s',
      gracefulStop: '15s',
    },

    // Publishers: 400 VUs start after subscribers are up, blast messages
    publishers: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '30s', target: 200 },    // Ramp publishers
        { duration: '30s', target: 400 },    // Full publisher pool
        { duration: '2m', target: 400 },     // Sustained fan-in
        { duration: '30s', target: 0 },      // Wind down
      ],
      startTime: '1m',  // Start after subscribers have connected
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

function generatePayload(size) {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  let result = '';
  for (let i = 0; i < size; i++) {
    result += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return result;
}

function openStream() {
  const connectStart = Date.now();

  try {
    client.connect(ADDR, {
      plaintext: true,
      timeout: '10s',
    });
  } catch (e) {
    connectErrors.add(1);
    connectionSuccess.add(0);
    return null;
  }

  connectLatency.add(Date.now() - connectStart);
  connectionSuccess.add(1);

  let stream;
  try {
    stream = new grpc.Stream(client, 'gentis.v1.GentisService/Stream');
  } catch (e) {
    streamErrors.add(1);
    client.close();
    return null;
  }

  stream.on('error', () => {
    streamErrors.add(1);
  });

  stream.write({ connect: { authToken: 'spike-test' } });
  sleep(0.1);

  return stream;
}

// ── Subscriber VU ──────────────────────────────────────────
// Each subscriber joins 1-2 hot channels and holds the connection open.
// With 1600 subscribers across 5 channels, each channel gets ~320-640 subscribers.
// A single publish fans out to all of them.
export function subscriber() {
  const vuId = __VU;

  const stream = openStream();
  if (!stream) {
    sleep(2);
    return;
  }

  let received = 0;
  let firstMessageTime = 0;
  const subscribeTime = Date.now();

  stream.on('data', (msg) => {
    if (msg.channelMessage) {
      if (received === 0) {
        firstMessageTime = Date.now();
        firstMessageLatency.add(firstMessageTime - subscribeTime);
      }
      received++;
      receivedMessages.add(1);
    }
  });

  // Subscribe to 1-2 hot channels
  const primaryChannel = HOT_CHANNELS[vuId % HOT_CHANNELS.length];
  stream.write({ subscribe: { channel: primaryChannel } });

  // 40% of subscribers also join a second channel
  if (vuId % 5 < 2) {
    const secondChannel = HOT_CHANNELS[(vuId + 1) % HOT_CHANNELS.length];
    stream.write({ subscribe: { channel: secondChannel } });
  }

  sleep(0.2);

  // Hold connection open — just sit and receive fan-out messages
  // Split into intervals so K6 can track progress
  for (let i = 0; i < 30; i++) {
    sleep(5);
    deliveryRate.add(received > 0 ? 1 : 0);
  }

  stream.end();
  client.close();
}

// ── Publisher VU ───────────────────────────────────────────
// Each publisher picks a hot channel and publishes repeatedly.
// With 400 publishers across 5 channels, each channel gets ~80 publishers
// all fanning-in to the same channel simultaneously.
export function publisher() {
  const vuId = __VU;

  const stream = openStream();
  if (!stream) {
    sleep(2);
    return;
  }

  stream.on('data', () => {});

  const targetChannel = HOT_CHANNELS[vuId % HOT_CHANNELS.length];

  // Subscribe too, so we can measure round-trip
  stream.write({ subscribe: { channel: targetChannel } });
  sleep(0.3);

  // Publish in bursts: 10 messages, short pause, repeat
  for (let burst = 0; burst < 20; burst++) {
    for (let i = 0; i < 10; i++) {
      stream.write({
        publish: {
          channel: targetChannel,
          data: generatePayload(PAYLOAD_SIZE),
        },
      });
      publishedMessages.add(1);
    }
    // Pause between bursts to let the server fan-out
    sleep(0.5);
  }

  // Hold a bit to drain remaining messages
  sleep(5);

  stream.end();
  client.close();
}

export function handleSummary(data) {
  const get = (name, field) => {
    const m = data.metrics[name];
    if (!m) return 'N/A';
    if (field === 'count') return m.values.count || 0;
    if (field === 'rate') return (m.values.rate * 100).toFixed(2) + '%';
    if (field) return (m.values[field] || 0).toFixed(2);
    return m.values;
  };

  console.log('\n╔══════════════════════════════════════════╗');
  console.log('║        SPIKE FAN-IN/FAN-OUT SUMMARY      ║');
  console.log('╠══════════════════════════════════════════╣');
  console.log(`║  Peak VUs:              ${String(get('vus_max', 'max')).padStart(14)} ║`);
  console.log(`║  Connect Errors:        ${String(get('connect_errors', 'count')).padStart(14)} ║`);
  console.log(`║  Stream Errors:         ${String(get('stream_errors', 'count')).padStart(14)} ║`);
  console.log(`║  Connection Success:    ${String(get('connection_success_rate', 'rate')).padStart(14)} ║`);
  console.log(`║  Connect Latency p95:   ${String(get('connect_latency_ms', 'p(95)') + 'ms').padStart(14)} ║`);
  console.log('╠══════════════════════════════════════════╣');
  console.log(`║  Published Messages:    ${String(get('published_messages', 'count')).padStart(14)} ║`);
  console.log(`║  Received Messages:     ${String(get('received_messages', 'count')).padStart(14)} ║`);
  console.log(`║  First Msg Latency p95: ${String(get('first_message_latency_ms', 'p(95)') + 'ms').padStart(14)} ║`);
  console.log(`║  Delivery Rate:         ${String(get('deliveryRate', 'rate') || get('message_delivery_rate', 'rate')).padStart(14)} ║`);
  console.log('╚══════════════════════════════════════════╝\n');

  return {};
}
