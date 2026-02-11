import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';
import { newClient, openStream, closeStream } from './lib/grpc.js';

const connectionFailures = new Counter('connection_failures');

const client = newClient();

export const options = {
  vus: 5,
  iterations: 10,
  thresholds: {
    checks: ['rate>0.99'],
    connection_failures: ['count<1'],
  },
};

export default function () {
  const channel = `smoke-${__VU}-${__ITER}`;
  const publishCount = 3;
  let received = 0;
  let subscribedOk = false;
  let unsubscribedOk = false;

  const conn = openStream(client, 'smoke-test', {
    onData(msg) {
      if (msg.channelMessage && msg.channelMessage.channel === channel) {
        received++;
      }
      if (msg.subscribed) subscribedOk = true;
      if (msg.unsubscribed) unsubscribedOk = true;
    },
    onError(err) {
      const s = String(err);
      if (!s.includes('canceled') && !s.includes('CANCELLED')) {
        connectionFailures.add(1);
      }
    },
  });

  if (!conn) {
    connectionFailures.add(1);
    check(null, { 'connection established': () => false });
    return;
  }

  check(null, { 'connection established': () => true });

  conn.stream.write({ subscribe: { channel } });
  sleep(0.3);

  for (let i = 0; i < publishCount; i++) {
    conn.stream.write({
      publish: { channel, data: `smoke-msg-${i}` },
    });
    sleep(0.1);
  }

  sleep(1);

  conn.stream.write({ unsubscribe: { channel } });
  sleep(0.3);

  check(null, {
    'subscribed confirmation received': () => subscribedOk,
    'unsubscribed confirmation received': () => unsubscribedOk,
  });

  closeStream(client, conn.stream);
}
