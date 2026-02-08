import grpc from 'k6/net/grpc';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
import { randomIntBetween } from 'https://jslib.k6.io/k6-utils/1.2.0/index.js';

const memoryCheckInterval = new Counter('memory_check_interval');
const connectionUptime = new Trend('connection_uptime_seconds');
const reconnectCount = new Counter('reconnect_count');
const messageDeliveryRate = new Rate('message_delivery_rate');

const SERVER_ADDR = __ENV.SERVER_ADDR || 'localhost:9000';
const RELAY_ADDR = __ENV.RELAY_ADDR || 'localhost:9001';
const TARGET = __ENV.TARGET || 'server';
const SOAK_DURATION = __ENV.SOAK_DURATION || '30m';

const ADDR = TARGET === 'relay' ? RELAY_ADDR : SERVER_ADDR;

const client = new grpc.Client();
client.load(['../../api/proto'], 'gentis/v1/gentis.proto');

function parseDurationToSeconds(duration) {
  const match = duration.match(/(\d+)([smh])/);
  if (!match) return 1800; // default 30 minutes
  const value = parseInt(match[1]);
  const unit = match[2];
  switch (unit) {
    case 's': return value;
    case 'm': return value * 60;
    case 'h': return value * 3600;
    default: return 1800;
  }
}

const totalSeconds = parseDurationToSeconds(SOAK_DURATION);

export const options = {
  scenarios: {
    soak: {
      executor: 'constant-vus',
      vus: 50,
      duration: SOAK_DURATION,
    },
  },
};

export default function() {
  const vuId = __VU;
  const channelName = `soak-channel-${vuId % 5}`;
  const connectionStart = Date.now();
  let messagesSent = 0;
  let messagesReceived = 0;

  try {
    client.connect(ADDR, {
      plaintext: true,
      timeout: '10s',
    });
  } catch (e) {
    reconnectCount.add(1);
    sleep(5);
    return;
  }

  const stream = new grpc.Stream(client, 'gentis.v1.GentisService/Stream');

  stream.on('data', (msg) => {
    if (msg.channelMessage) {
      messagesReceived++;
    }
  });

  stream.on('error', () => {
    reconnectCount.add(1);
  });

  stream.write({ connect: { authToken: 'soak-test' } });
  sleep(0.5);

  stream.write({ subscribe: { channel: channelName } });
  sleep(1);

  const iterations = Math.floor(totalSeconds / 10); // Activity every 10 seconds

  for (let i = 0; i < iterations; i++) {
    const payload = `soak-test-${vuId}-${i}-${Date.now()}`;
    stream.write({
      publish: {
        channel: channelName,
        data: payload,
      },
    });
    messagesSent++;

    if (i % 30 === 0 && vuId % 3 === 0) {
      const tempChannel = `temp-channel-${vuId}`;
      stream.write({ subscribe: { channel: tempChannel } });
      sleep(2);
      stream.write({ unsubscribe: { channel: tempChannel } });
    }

    if (i % 10 === 0) {
      stream.write({ ping: {} });
    }

    sleep(randomIntBetween(5, 15));
  }

  const uptime = (Date.now() - connectionStart) / 1000;
  connectionUptime.add(uptime);

  if (messagesSent > 0) {
    messageDeliveryRate.add(messagesReceived / messagesSent);
  }

  stream.end();
  client.close();
}
