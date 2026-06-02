import { setTimeout } from 'k6/timers';

export function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function envInt(name, def) {
  return parseInt(__ENV[name] || String(def), 10);
}

const ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';

export function generatePayload(size) {
  if (size <= 0) return '';
  const parts = [];
  let remaining = size;
  while (remaining > 0) {
    const take = Math.min(remaining, ALPHABET.length);
    parts.push(ALPHABET.substring(0, take));
    remaining -= take;
  }
  return parts.join('');
}

const DURATION_UNIT_SECONDS = { s: 1, m: 60, h: 3600 };

export function durationToSeconds(d) {
  const m = /^(\d+)([smh])$/.exec(d);
  if (!m) return 1800;
  return parseInt(m[1], 10) * DURATION_UNIT_SECONDS[m[2]];
}
