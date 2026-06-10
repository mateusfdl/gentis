import { check } from 'k6';
import { Counter } from 'k6/metrics';
import { authToken } from './lib/auth.js';
import { delay } from './lib/util.js';
import { newClient, openStream, closeStream } from './lib/grpc.js';
import encoding from 'k6/encoding';

const connectionFailures = new Counter('connection_failures');

const client = newClient();

export const options = {
  vus: 3,
  iterations: 6,
  thresholds: {
    checks: ['rate>0.99'],
    connection_failures: ['count<1'],
  },
};

export default async function () {
  const channel = `jobs:qos-${__VU}-${__ITER}`;
  const total = 10;
  const offsets = [];
  let subscribed = false;

  const conn = openStream(client, authToken(), {
    onData(msg) {
      if (msg.subscribed) subscribed = true;
      if (msg.channelMessage && msg.channelMessage.channel === channel) {
        offsets.push(Number(msg.channelMessage.offset));
        conn.stream.write({
          confirm: { channel, offset: msg.channelMessage.offset },
        });
      }
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

  conn.stream.write({
    subscribe: { channel, maxUnconfirmed: { count: 2 } },
  });
  await delay(300);

  const pubConn = openStream(client, authToken(), {});
  if (!pubConn) {
    connectionFailures.add(1);
    check(null, { 'publisher connection established': () => false });
    closeStream(client, conn.stream);
    return;
  }
  for (let i = 1; i <= total; i++) {
    pubConn.stream.write({
      publish: { channel, data: encoding.b64encode(`job-${i}`) },
    });
    await delay(20);
  }

  await delay(1500);

  check(null, {
    'subscribed with qos window': () => subscribed,
    'all publications delivered': () => offsets.length === total,
    'deliveries ordered without gaps': () =>
      offsets.every((off, i) => off === offsets[0] + i),
  });

  closeStream(client, pubConn.stream);
  closeStream(client, conn.stream);
}
