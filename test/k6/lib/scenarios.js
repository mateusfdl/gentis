import { envInt } from './util.js';

export function pubsubScenarios() {
  const rampTarget = envInt('RAMP_TARGET', 200);
  const spikeTarget = envInt('SPIKE_TARGET', 500);
  return {
    steady_load: {
      executor: 'constant-vus',
      vus: envInt('STEADY_VUS', 100),
      duration: envInt('STEADY_DURATION', 60) + 's',
      startTime: '0s',
      gracefulStop: '15s',
    },
    ramp_up: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '30s', target: rampTarget },
        { duration: '30s', target: rampTarget },
        { duration: '10s', target: 0 },
      ],
      startTime: '70s',
      gracefulRampDown: '15s',
      gracefulStop: '15s',
    },
    spike: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '10s', target: spikeTarget },
        { duration: '30s', target: spikeTarget },
        { duration: '10s', target: 0 },
      ],
      startTime: '150s',
      gracefulRampDown: '15s',
      gracefulStop: '15s',
    },
  };
}

export const stressStages = [
  { duration: '2m', target: 100 },
  { duration: '5m', target: 100 },
  { duration: '2m', target: 200 },
  { duration: '5m', target: 200 },
  { duration: '2m', target: 400 },
  { duration: '5m', target: 400 },
  { duration: '2m', target: 600 },
  { duration: '5m', target: 600 },
  { duration: '5m', target: 0 },
];

export function spikeScenarios() {
  const subVUs = envInt('SPIKE_SUB_VUS', 1600);
  const pubVUs = envInt('SPIKE_PUB_VUS', 400);
  const subRamp = Math.max(1, Math.round(subVUs / 2));
  const pubRamp = Math.max(1, Math.round(pubVUs / 2));
  return {
    subscribers: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '30s', target: subRamp },
        { duration: '30s', target: subVUs },
        { duration: '3m', target: subVUs },
        { duration: '30s', target: 0 },
      ],
      exec: 'subscriber',
      gracefulRampDown: '15s',
      gracefulStop: '15s',
    },
    publishers: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '30s', target: pubRamp },
        { duration: '30s', target: pubVUs },
        { duration: '2m', target: pubVUs },
        { duration: '30s', target: 0 },
      ],
      startTime: '1m',
      exec: 'publisher',
      gracefulRampDown: '15s',
      gracefulStop: '15s',
    },
  };
}
