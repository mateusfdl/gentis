import { check } from 'k6';
import { Counter } from 'k6/metrics';
import { authToken } from './lib/auth.js';
import { delay } from './lib/util.js';
import { newClient, openStream, closeStream, subscribe, unsubscribe, publish } from './lib/grpc.js';

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

export default async function () {
  const channel = `smoke-${__VU}-${__ITER}`;
  let subscribedOk = false;
  let unsubscribedOk = false;
  let selfEchoes = 0;
  const acks = [];

  const conn = openStream(client, authToken(), {
    onData(msg) {
      if (msg.subscribed) subscribedOk = true;
      if (msg.unsubscribed) unsubscribedOk = true;
      if (msg.channelMessage && msg.channelMessage.channel === channel) selfEchoes++;
      if (msg.published && msg.published.channel === channel) acks.push(msg.published);
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

  subscribe(conn.stream, channel);
  await delay(300);

  for (let i = 0; i < 3; i++) {
    publish(conn.stream, channel, `smoke-msg-${i}`, `pub-${i}`);
    await delay(100);
  }

  await delay(1000);

  unsubscribe(conn.stream, channel);
  await delay(300);

  check(null, {
    'subscribed confirmation received': () => subscribedOk,
    'unsubscribed confirmation received': () => unsubscribedOk,
    'publisher excluded from own publish': () => selfEchoes === 0,
    'every publish acked': () => acks.length === 3,
    'ack offsets are monotonic per channel': () =>
      acks.every((a, i) => Number(a.offset) === Number(acks[0].offset) + i),
    'acks share one epoch': () =>
      acks.length > 0 && acks.every((a) => a.epoch === acks[0].epoch && Number(a.epoch) !== 0),
  });

  closeStream(client, conn.stream);
}
