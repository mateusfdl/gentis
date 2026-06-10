import grpc from 'k6/net/grpc';
import encoding from 'k6/encoding';
import { ADDR, PROTO_DIR, PROTO_FILE, SERVICE_METHOD } from './config.js';

export function newClient() {
  const c = new grpc.Client();
  c.load([PROTO_DIR], PROTO_FILE);
  return c;
}

export function openStream(client, authToken, { onError, onData, metadata } = {}) {
  try {
    client.connect(ADDR, { plaintext: true, timeout: '10s' });
  } catch (e) {
    return null;
  }

  let stream;
  try {
    const opts = metadata ? { metadata } : {};
    stream = new grpc.Stream(client, SERVICE_METHOD, opts);
  } catch (e) {
    client.close();
    return null;
  }

  if (onError) stream.on('error', onError);
  if (onData) stream.on('data', onData);

  stream.write({ connect: { authToken } });

  return { client, stream };
}

export function closeStream(client, stream) {
  try { stream.end(); } catch (_) {}
  try { client.close(); } catch (_) {}
}

export function subscribe(stream, channel) {
  stream.write({ subscribe: { channel } });
}

export function unsubscribe(stream, channel) {
  stream.write({ unsubscribe: { channel } });
}

export function publish(stream, channel, body, id = '') {
  stream.write({ id, publish: { channel, data: encoding.b64encode(String(body)) } });
}

export function ping(stream) {
  stream.write({ ping: {} });
}

export function channelData(msg) {
  if (!msg || !msg.channelMessage) return null;
  const d = msg.channelMessage.data;
  if (d == null) return null;
  return encoding.b64decode(d, 'std', 's');
}
