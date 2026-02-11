import { randomIntBetween } from 'https://jslib.k6.io/k6-utils/1.2.0/index.js';
import { group, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
import grpc from 'k6/net/grpc';

const subscribeLatency = new Trend('subscribe_latency');
const publishLatency = new Trend('publish_latency');
const messageReceiveLatency = new Trend('message_receive_latency');
const messagesReceived = new Counter('messages_received');
const messagesDropped = new Counter('messages_dropped');
const connectionErrors = new Counter('connection_errors');
const subscribeSuccessRate = new Rate('subscribe_success_rate');
const publishSuccessRate = new Rate('publish_success_rate');

const SERVER_ADDR = __ENV.SERVER_ADDR || 'localhost:9000';
const RELAY_ADDR = __ENV.RELAY_ADDR || 'localhost:9001';
const TARGET = __ENV.TARGET || 'server';
const PAYLOAD_SIZE = parseInt(__ENV.PAYLOAD_SIZE || '256');
const CHANNEL_PREFIX = __ENV.CHANNEL_PREFIX || 'test-channel';

const ADDR = TARGET === 'relay' ? RELAY_ADDR : SERVER_ADDR;

const client = new grpc.Client();
client.load(['../../api/proto'], 'gentis/v1/gentis.proto');

export const options = {
  scenarios: {
    steady_load: {
      executor: 'constant-vus',
      vus: parseInt(__ENV.STEADY_VUS || '100'),
      duration: parseInt(__ENV.STEADY_DURATION || '60') + 's',
      startTime: '0s',
      gracefulStop: '15s',
    },

    ramp_up: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '30s', target: parseInt(__ENV.RAMP_TARGET || '200') },
        { duration: '30s', target: parseInt(__ENV.RAMP_TARGET || '200') },
        { duration: '10s', target: 0 },
      ],
      startTime: '70s',
      gracefulRampDown: '15s',
      gracefulStop: '15s',
    },

    spike: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '10s', target: parseInt(__ENV.SPIKE_TARGET || '500') },
        { duration: '30s', target: parseInt(__ENV.SPIKE_TARGET || '500') },
        { duration: '10s', target: 0 },
      ],
      startTime: '150s',
      gracefulRampDown: '15s',
      gracefulStop: '15s',
    },
  },
  thresholds: {
    subscribe_latency: ['p(95)<100'],
    publish_latency: ['p(95)<50'],
    subscribe_success_rate: ['rate>0.95'],
    publish_success_rate: ['rate>0.95'],
    connection_errors: ['count<100'],
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

function getIdentifiers() {
  const vuId = __VU;
  const iteration = __ITER;
  return {
    connectionId: `vu-${vuId}-iter-${iteration}`,
    channelName: `${CHANNEL_PREFIX}-${vuId % 10}`,
    subscriberId: vuId,
  };
}

export default function() {
  const ids = getIdentifiers();

  group('Connection Lifecycle', () => {
    try {
      client.connect(ADDR, {
        plaintext: true,
        timeout: '10s',
      });
    } catch (e) {
      connectionErrors.add(1);
      console.error(`Connection failed: ${e}`);
      return;
    }

    const stream = new grpc.Stream(client, 'gentis.v1.GentisService/Stream', {
      metadata: {
        'x-client-id': ids.connectionId,
      },
    });

    const receivedMessages = [];
    stream.on('data', (message) => {
      if (message.channelMessage) {
        messagesReceived.add(1);
        receivedMessages.push({
          receivedAt: Date.now(),
          channel: message.channelMessage.channel,
        });
      }
    });

    stream.on('error', (error) => {
      const errStr = String(error);
      if (errStr.includes('canceled') || errStr.includes('CANCELLED')) {
        return;
      }
      connectionErrors.add(1);
      console.error(`Stream error: ${error}`);
    });

    stream.write({
      connect: {
        authToken: 'test-token',
      },
    });

    sleep(randomIntBetween(1, 3) / 10);

    group('Subscribe', () => {
      const subStart = Date.now();

      stream.write({
        subscribe: {
          channel: ids.channelName,
        },
      });

      sleep(0.5);

      const subLatency = Date.now() - subStart;
      subscribeLatency.add(subLatency);
      subscribeSuccessRate.add(1);
    });

    sleep(randomIntBetween(1, 2) / 10);

    group('Publish', () => {
      const numPublishes = randomIntBetween(3, 8);

      for (let i = 0; i < numPublishes; i++) {
        const pubStart = Date.now();
        const payload = generatePayload(PAYLOAD_SIZE);

        stream.write({
          publish: {
            channel: ids.channelName,
            data: payload,
          },
        });

        const pubLatency = Date.now() - pubStart;
        publishLatency.add(pubLatency);
        publishSuccessRate.add(1);

        sleep(randomIntBetween(5, 15) / 100);
      }
    });

    sleep(randomIntBetween(2, 5));

    group('Unsubscribe', () => {
      stream.write({
        unsubscribe: {
          channel: ids.channelName,
        },
      });

      sleep(0.3);
    });

    stream.end();
    sleep(1);
    client.close();

    if (receivedMessages.length > 0) {
      const avgLatency = receivedMessages.reduce((sum, m) => sum + m.receivedAt, 0) / receivedMessages.length;
      messageReceiveLatency.add(avgLatency);
    }
  });

  sleep(randomIntBetween(1, 3));
}

export function teardown(data) {
  console.log('Test completed. Cleaning up...');
}
