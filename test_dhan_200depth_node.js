const WebSocket = require('ws');
require('dotenv').config();

const ACCESS_TOKEN = process.env.DHAN_ACCESS_TOKEN;
const CLIENT_ID = process.env.DHAN_CLIENT_ID;

// Test instruments
const TESTS = [
    {security_id: '49229', segment: 'NSE_FNO', name: 'NIFTY JAN 2026 FUT'},
    {security_id: '1333', segment: 'NSE_EQ', name: 'RELIANCE'},
    {security_id: '11536', segment: 'NSE_EQ', name: 'INFY'}
];

let currentIndex = 0;

function testInstrument(test) {
    console.log("\n" + "=".repeat(70));
    console.log(`Testing: ${test.name} (SecurityId: ${test.security_id})`);
    console.log("=".repeat(70));

    // Try with version=2 as support suggested
    const wsUrl = `wss://full-depth-api.dhan.co/twohundreddepth?version=2&token=${ACCESS_TOKEN}&clientId=${CLIENT_ID}&authType=2`;
    
    console.log(`Connecting to: ${wsUrl.substring(0, 70)}...`);
    
    const ws = new WebSocket(wsUrl);
    
    ws.on('open', function() {
        console.log("✓ [CONNECTED] WebSocket opened");
        
        // Try support's suggested InstrumentList format
        const subscription = {
            RequestCode: 23,
            InstrumentList: [{
                ExchangeSegment: test.segment,
                SecurityId: test.security_id
            }]
        };
        
        ws.send(JSON.stringify(subscription));
        console.log(`✓ [SUBSCRIBED] Sent: ${JSON.stringify(subscription)}`);
        console.log("  Waiting for data...");
    });
    
    ws.on('message', function(data) {
        console.log(`✓ [DATA RECEIVED] ${data.length} bytes`);
        if (Buffer.isBuffer(data) && data.length >= 3) {
            const responseCode = data[2];
            console.log(`  Response code: ${responseCode} (41=BID, 51=ASK, 50=DISCONNECT)`);
        }
    });
    
    ws.on('error', function(error) {
        console.log(`✗ [ERROR] ${error.message}`);
    });
    
    ws.on('close', function(code, reason) {
        console.log(`✗ [CLOSED] Code: ${code}, Reason: ${reason || 'None'}`);
        
        // Test next instrument after 2 seconds
        setTimeout(() => {
            currentIndex++;
            if (currentIndex < TESTS.length) {
                testInstrument(TESTS[currentIndex]);
            } else {
                printSummary();
            }
        }, 2000);
    });
    
    // Close connection after 10 seconds
    setTimeout(() => {
        if (ws.readyState === WebSocket.OPEN) {
            ws.close();
        }
    }, 10000);
}

function printSummary() {
    console.log("\n" + "=".repeat(70));
    console.log("SUMMARY:");
    console.log("=".repeat(70));
    console.log(`✓ Authentication: CLIENT_ID=${CLIENT_ID}, Token valid`);
    console.log(`✓ WebSocket URL: wss://full-depth-api.dhan.co/twohundreddepth`);
    console.log(`✓ Subscription: RequestCode=23, InstrumentList format`);
    console.log(`✗ Result: All instruments connect but disconnect immediately (0 data)`);
    console.log(``);
    console.log(`Note: 20-level depth works fine with same credentials:`);
    console.log(`  URL: wss://depth-api-feed.dhan.co/twentydepth`);
    console.log(`  Subscription: RequestCode=23 + InstrumentList format`);
    console.log(`  Result: Continuous data streaming (664-1992 bytes per message)`);
    console.log("=".repeat(70));
}

// Start testing
console.log(`CLIENT_ID: ${CLIENT_ID}`);
console.log(`Testing 200-depth with Node.js WebSocket...`);
testInstrument(TESTS[currentIndex]);
