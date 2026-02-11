export const SERVER_ADDR = __ENV.SERVER_ADDR || 'localhost:9000';
export const RELAY_ADDR = __ENV.RELAY_ADDR || 'localhost:9001';
export const TARGET = __ENV.TARGET || 'server';
export const ADDR = TARGET === 'relay' ? RELAY_ADDR : SERVER_ADDR;
export const PAYLOAD_SIZE = parseInt(__ENV.PAYLOAD_SIZE || '256');
export const CHANNEL_PREFIX = __ENV.CHANNEL_PREFIX || 'test-channel';

export const PROTO_DIR = '../../api/proto';
export const PROTO_FILE = 'gentis/v1/gentis.proto';
export const SERVICE_METHOD = 'gentis.v1.GentisService/Stream';
