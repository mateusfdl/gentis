import { check } from 'k6';
import { Counter } from 'k6/metrics';
import { WS_AUTH_TOKEN } from './lib/config.js';
import { delay } from './lib/util.js';
import { openWS, subscribe, unsubscribe, publish, close } from './lib/ws.js';

const connectionFailures = new Counter('connection_failures');

export const options = {
  vus: 5,
  iterations: 10,
  tags: { transport: 'ws' },
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

  let ws;
  try {
    ws = await openWS(WS_AUTH_TOKEN, {
      onMessage(msg) {
        if (msg.subscribed) subscribedOk = true;
        if (msg.unsubscribed) unsubscribedOk = true;
        if (msg.channel_message && msg.channel_message.channel === channel) selfEchoes++;
        if (msg.published && msg.published.channel === channel) acks.push(msg.published);
      },
    });
  } catch (e) {
    connectionFailures.add(1);
    check(null, { 'connection established': () => false });
    return;
  }

  check(null, { 'connection established': () => true });

  subscribe(ws, channel, 'sub');
  await delay(300);

  for (let i = 0; i < 3; i++) {
    publish(ws, channel, `smoke-msg-${i}`, `pub-${i}`);
    await delay(100);
  }

  await delay(1000);

  unsubscribe(ws, channel, 'unsub');
  await delay(300);

  check(null, {
    'subscribed confirmation received': () => subscribedOk,
    'unsubscribed confirmation received': () => unsubscribedOk,
    'publisher excluded from own publish': () => selfEchoes === 0,
    'every publish acked': () => acks.length === 3,
    'ack offsets are monotonic per channel': () =>
      acks.every((a, i) => Number(a.offset) === i + 1),
    'acks share one epoch': () =>
      acks.length > 0 && acks.every((a) => a.epoch === acks[0].epoch && Number(a.epoch) !== 0),
  });

  close(ws);
  await delay(100);
}
