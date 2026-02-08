import grpc from 'k6/net/grpc';
import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';

// Stress test - pushes the system to its limits
const connectErrors = new Counter('connect_errors');
const subscribeTimeouts = new Counter('subscribe_timeouts');
const publishTimeouts = new Counter('publish_timeouts');
const activeConnections = new Trend('active_connections');

const SERVER_ADDR = __ENV.SERVER_ADDR || 'localhost:9000';
const RELAY_ADDR = __ENV.RELAY_ADDR || 'localhost:9001';
const TARGET = __ENV.TARGET || 'server';
const PAYLOAD_SIZE = parseInt(__ENV.PAYLOAD_SIZE || '1024');

const ADDR = TARGET === 'relay' ? RELAY_ADDR : SERVER_ADDR;

const client = new grpc.Client();
client.load(['../../api/proto'], 'gentis/v1/gentis.proto');

export const options = {
  // Stress test configuration
  stages: [
    { duration: '2m', target: 100 },     // Ramp up to 100 VUs
    { duration: '5m', target: 100 },     // Stay at 100 VUs
    { duration: '2m', target: 200 },     // Ramp up to 200 VUs
    { duration: '5m', target: 200 },     // Stay at 200 VUs
    { duration: '2m', target: 400 },     // Ramp up to 400 VUs
    { duration: '5m', target: 400 },     // Stay at 400 VUs
    { duration: '2m', target: 600 },     // Ramp up to 600 VUs
    { duration: '5m', target: 600 },     // Stay at 600 VUs
    { duration: '5m', target: 0 },       // Ramp down
  ],
  thresholds: {
    connect_errors: ['count<50'],
  },
};

function generatePayload(size) {
  return 'x'.repeat(size);
}

export default function () {
  const vuId = __VU;
  const iteration = __ITER;
  const channelName = `stress-channel-${vuId % 20}`;

  // Connect with timeout
  try {
    client.connect(ADDR, {
      plaintext: true,
      timeout: '5s',
    });
  } catch (e) {
    connectErrors.add(1);
    return;
  }

  const stream = new grpc.Stream(client, 'gentis.v1.GentisService/Stream');

  let messagesReceived = 0;
  stream.on('data', (msg) => {
    if (msg.channelMessage) {
      messagesReceived++;
    }
  });

  stream.on('error', () => {
    connectErrors.add(1);
  });

  // Authenticate
  stream.write({ connect: { authToken: 'stress-test' } });
  sleep(0.2);

  // Subscribe with multiple channels for some VUs
  const numSubscriptions = vuId % 5 === 0 ? 3 : 1;
  for (let i = 0; i < numSubscriptions; i++) {
    const ch = `${channelName}-${i}`;
    stream.write({ subscribe: { channel: ch } });
  }
  sleep(0.5);

  // Stress publish loop - high frequency
  const publishCount = 20;
  for (let i = 0; i < publishCount; i++) {
    const payload = generatePayload(PAYLOAD_SIZE);
    stream.write({
      publish: {
        channel: channelName,
        data: payload,
      }
    });

    // Very short sleep to maximize throughput
    sleep(0.05);
  }

  // Wait to receive messages
  sleep(2);

  // Cleanup
  stream.end();
  client.close();

  activeConnections.add(messagesReceived);
}
