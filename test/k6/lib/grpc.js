import { sleep } from 'k6';
import grpc from 'k6/net/grpc';
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
  sleep(0.1);

  return { client, stream };
}

export function closeStream(client, stream) {
  try { stream.end(); } catch (_) {}
  try { client.close(); } catch (_) {}
}

const ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
export function generatePayload(size) {
  const parts = [];
  const chunkSize = ALPHABET.length;
  while (size > 0) {
    const take = Math.min(size, chunkSize);
    parts.push(ALPHABET.substring(0, take));
    size -= take;
  }
  return parts.join('');
}
