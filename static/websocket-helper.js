/**
 * Creates a WebSocket URL based on the current protocol
 * Uses wss:// for HTTPS (production) and ws:// for HTTP (development)
 */
function getWebSocketURL(path = '/ws') {
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  return protocol + "//" + window.location.host + path;
}
