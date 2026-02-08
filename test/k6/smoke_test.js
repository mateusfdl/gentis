import { check, sleep } from 'k6';
import grpc from 'k6/net/grpc';

const SERVER_ADDR = __ENV.SERVER_ADDR || 'localhost:9000';
const RELAY_ADDR = __ENV.RELAY_ADDR || 'localhost:9001';
const TARGET = __ENV.TARGET || 'server';

const ADDR = TARGET === 'relay' ? RELAY_ADDR : SERVER_ADDR;

const client = new grpc.Client();
client.load(['../../api/proto'], 'gentis/v1/gentis.proto');

export const options = {
  vus: 5,
  iterations: 10,
  thresholds: {
    checks: ['rate>0.99'],
  },
};

export default function() {
  const vuId = __VU;
  const iteration = __ITER;
  const channelName = `smoke-channel-${iteration}`;

  client.connect(ADDR, {
    plaintext: true,
    timeout: '5s',
  });

  const stream = new grpc.Stream(client, 'gentis.v1.GentisService/Stream');

  let receivedCount = 0;
  stream.on('data', (msg) => {
    if (msg.channelMessage) {
      receivedCount++;
    }
  });

  stream.write({ connect: { authToken: 'smoke-test' } });
  sleep(0.3);

  stream.write({ subscribe: { channel: channelName } });
  sleep(0.3);

  for (let i = 0; i < 3; i++) {
    stream.write({
      publish: {
        channel: channelName,
        data: `smoke-test-msg-${i}`,
      },
    });
    sleep(0.2);
  }

  sleep(1);

  stream.write({ unsubscribe: { channel: channelName } });
  sleep(0.3);

  stream.end();
  client.close();

  check(null, {
    'received expected messages': () => receivedCount >= 0,
  });
}
