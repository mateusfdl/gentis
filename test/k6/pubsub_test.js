import { check, group, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
import { randomIntBetween } from 'https://jslib.k6.io/k6-utils/1.2.0/index.js';
import { CHANNEL_PREFIX, PAYLOAD_SIZE } from './lib/config.js';
import { newClient, openStream, closeStream, generatePayload } from './lib/grpc.js';

const subscribeLatency = new Trend('subscribe_latency', true);
const publishLatency = new Trend('publish_latency', true);
const messageLatency = new Trend('message_latency', true);
const messagesReceived = new Counter('messages_received');
const connectionErrors = new Counter('connection_errors');
const subscribeSuccess = new Rate('subscribe_success_rate');
const publishSuccess = new Rate('publish_success_rate');

const client = newClient();

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

export default function () {
  const channel = `${CHANNEL_PREFIX}-${__VU % 10}`;
  let subscribedOk = false;

  const conn = openStream(client, 'test-token', {
    onData(msg) {
      if (msg.subscribed && msg.subscribed.channel === channel) {
        subscribedOk = true;
      }
      if (msg.channelMessage) {
        messagesReceived.add(1);
        const data = String(msg.channelMessage.data);
        const ts = parseInt(data.split('|')[0], 10);
        if (ts > 0) {
          messageLatency.add(Date.now() - ts);
        }
      }
    },
    onError(err) {
      const s = String(err);
      if (!s.includes('canceled') && !s.includes('CANCELLED')) {
        connectionErrors.add(1);
      }
    },
    metadata: { 'x-client-id': `vu-${__VU}-iter-${__ITER}` },
  });

  if (!conn) {
    connectionErrors.add(1);
    subscribeSuccess.add(0);
    publishSuccess.add(0);
    return;
  }

  sleep(randomIntBetween(1, 3) / 10);

  group('subscribe', () => {
    const t0 = Date.now();
    conn.stream.write({ subscribe: { channel } });
    sleep(0.3);
    subscribeLatency.add(Date.now() - t0);
    subscribeSuccess.add(subscribedOk ? 1 : 0);
  });

  sleep(randomIntBetween(1, 2) / 10);

  group('publish', () => {
    const count = randomIntBetween(3, 8);
    for (let i = 0; i < count; i++) {
      const ts = Date.now();
      const body = `${ts}|${generatePayload(PAYLOAD_SIZE)}`;

      const t0 = Date.now();
      try {
        conn.stream.write({ publish: { channel, data: body } });
        publishLatency.add(Date.now() - t0);
        publishSuccess.add(1);
      } catch (_) {
        publishSuccess.add(0);
      }

      sleep(randomIntBetween(5, 15) / 100);
    }
  });

  sleep(randomIntBetween(2, 5));

  group('unsubscribe', () => {
    conn.stream.write({ unsubscribe: { channel } });
    sleep(0.3);
  });

  closeStream(client, conn.stream);
  sleep(randomIntBetween(1, 3));
}
