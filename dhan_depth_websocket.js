const WebSocket = require('ws');

const clientId = '1109719771';
const accessToken = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzY3MDc2MDI1LCJpYXQiOjE3NjY5ODk2MjUsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA5NzE5NzcxIn0.YjrUkXL5YlXWBBvoiXaMBnbHGcnWFX73jgCn9iH95uTjdfcIRMwm7nR5v3ZSn0BCwFha5qPhLI7d_cRAsnpT-w';

const wsUrl = `wss://full-depth-api.dhan.co/twohundreddepth?token=${accessToken}&clientId=${clientId}&authType=2`;

const ws = new WebSocket(wsUrl);

ws.on('open', function open() {
  console.log('Connected to Dhan 200-Level Depth WebSocket');

  // Send subscription message for depth data after a short delay
  setTimeout(() => {
    const subscribeMessage = {
      "RequestCode": 23,
      "ExchangeSegment": "NSE_FNO",
      "SecurityId": "49229"
    };
    ws.send(JSON.stringify(subscribeMessage));
    console.log('Subscription sent for NIFTY JAN 2025 FUT (49229) - 200 level depth');
  }, 1000);
});

ws.on('message', function incoming(data) {
  // Data is binary buffer
  console.log('Received binary depth data:', data.length, 'bytes');
  // For simple printing, log the buffer as hex or something
  console.log(data.toString('hex').substring(0, 100) + '...'); // First 100 chars of hex
});

ws.on('error', function error(err) {
  console.error('WebSocket error:', err);
});

ws.on('close', function close(code, reason) {
  console.log('WebSocket closed:', code, reason.toString());
});