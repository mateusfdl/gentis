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
  return {
    subscribers: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '30s', target: 800 },
        { duration: '30s', target: 1600 },
        { duration: '3m', target: 1600 },
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
        { duration: '30s', target: 200 },
        { duration: '30s', target: 400 },
        { duration: '2m', target: 400 },
        { duration: '30s', target: 0 },
      ],
      startTime: '1m',
      exec: 'publisher',
      gracefulRampDown: '15s',
      gracefulStop: '15s',
    },
  };
}
