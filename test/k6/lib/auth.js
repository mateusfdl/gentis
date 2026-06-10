import crypto from 'k6/crypto';
import encoding from 'k6/encoding';

export function signToken(secret, claims) {
  const header = encoding.b64encode(JSON.stringify({ alg: 'HS256', typ: 'JWT' }), 'rawurl');
  const payload = encoding.b64encode(JSON.stringify(claims), 'rawurl');
  const signingInput = `${header}.${payload}`;
  const sig = crypto.hmac('sha256', secret, signingInput, 'base64rawurl');
  return `${signingInput}.${sig}`;
}

export function authToken() {
  const secret = __ENV.AUTH_HMAC_SECRET;
  if (!secret) return __ENV.AUTH_TOKEN || 'k6-token';
  return signToken(secret, {
    sub: `k6-vu-${__VU}`,
    exp: Math.floor(Date.now() / 1000) + 3600,
  });
}
