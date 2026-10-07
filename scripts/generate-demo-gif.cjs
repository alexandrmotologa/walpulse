const http = require('http');
const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');
const puppeteer = require('C:/Users/alexander/.gemini/antigravity-ide/scratch/apiscope-extension/node_modules/puppeteer-core');

const CHROME_PATH = 'C:/Program Files/Google/Chrome/Application/chrome.exe';
const PORT = 8844;
const HTML_FILE = path.resolve(__dirname, '../src/main/resources/static/dashboard/index.html');
const IMAGES_DIR = path.resolve(__dirname, '../docs/images');
const FRAMES_DIR = path.resolve(__dirname, '../.temp_frames');
const OUTPUT_GIF = path.join(IMAGES_DIR, 'walpulse-demo.gif');

let eventsCount = 18420;
let commitsCount = 4210;

function createServer() {
  const htmlContent = fs.readFileSync(HTML_FILE, 'utf8');

  const server = http.createServer((req, res) => {
    // CORS headers
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

    if (req.method === 'OPTIONS') {
      res.writeHead(200);
      res.end();
      return;
    }

    const parsedUrl = new URL(req.url, `http://localhost:${PORT}`);
    const pathname = parsedUrl.pathname;

    if (pathname === '/' || pathname === '/dashboard') {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(htmlContent);
      return;
    }

    if (pathname === '/api/v1/replication/status') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        state: 'STREAMING',
        totalEventsProcessed: eventsCount,
        totalTransactionsCommitted: commitsCount,
        lagBytes: 0,
        slotName: 'walpulse_prod_slot',
        publicationName: 'walpulse_all_tables',
        lastReceivedLsn: '0/18A42F0',
        lastFlushedLsn: '0/18A42F0'
      }));
      return;
    }

    if (pathname === '/api/v1/replication/events') {
      res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive'
      });
      res.write('data: {"type":"CONNECTED","state":"STREAMING"}\n\n');
      return;
    }

    if (pathname === '/api/v1/schemas') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify([
        {
          schemaName: 'public',
          tableName: 'orders',
          relationOid: 16402,
          columns: [
            { name: 'id', isKey: true, typeOid: 'INT8' },
            { name: 'customer_id', isKey: false, typeOid: 'INT8' },
            { name: 'total_amount', isKey: false, typeOid: 'NUMERIC(12,2)' },
            { name: 'status', isKey: false, typeOid: 'VARCHAR(32)' },
            { name: 'created_at', isKey: false, typeOid: 'TIMESTAMPTZ' }
          ]
        },
        {
          schemaName: 'public',
          tableName: 'outbox_messages',
          relationOid: 16410,
          columns: [
            { name: 'id', isKey: true, typeOid: 'UUID' },
            { name: 'aggregate_type', isKey: false, typeOid: 'VARCHAR(64)' },
            { name: 'aggregate_id', isKey: false, typeOid: 'VARCHAR(64)' },
            { name: 'destination_topic', isKey: false, typeOid: 'VARCHAR(128)' }
          ]
        },
        {
          schemaName: 'public',
          tableName: 'customers',
          relationOid: 16395,
          columns: [
            { name: 'id', isKey: true, typeOid: 'INT8' },
            { name: 'email', isKey: false, typeOid: 'VARCHAR(255)' },
            { name: 'loyalty_tier', isKey: false, typeOid: 'VARCHAR(32)' }
          ]
        }
      ]));
      return;
    }

    if (pathname === '/api/v1/schemas/history') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify([
        {
          schemaName: 'public',
          tableName: 'orders',
          timestamp: Date.now() - 45000,
          description: 'ALTER TABLE orders ADD COLUMN payment_ref VARCHAR(64)'
        },
        {
          schemaName: 'public',
          tableName: 'customers',
          timestamp: Date.now() - 180000,
          description: "ALTER TABLE customers ADD COLUMN loyalty_tier VARCHAR(32) DEFAULT 'SILVER'"
        }
      ]));
      return;
    }

    if (pathname === '/api/v1/dlq') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify([
        {
          id: 'dlq_9914',
          sinkRecord: { destination: 'kafka://orders.events.paid' },
          failedAt: Date.now() - 32000,
          errorReason: 'BrokerConnectionTimeout: kafka-broker-02.prod:9092'
        }
      ]));
      return;
    }

    if (pathname === '/api/v1/config/sandbox') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        output: {
          id: 10042,
          customer_email: 'a***@domain.com',
          credit_card: '4111-****-****-1111',
          total_amount: 289.5,
          status: 'APPROVED'
        }
      }));
      return;
    }

    if (pathname.startsWith('/api/v1/simulate/')) {
      eventsCount += (pathname.includes('burst') ? 50 : 1);
      commitsCount += 1;
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ success: true, count: 1 }));
      return;
    }

    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true }));
  });

  return server;
}

function sleep(ms) {
  return new Promise(res => setTimeout(res, ms));
}

async function run() {
  console.log('1. Starting mock HTTP server...');
  const server = createServer();
  await new Promise(res => server.listen(PORT, res));
  console.log(`Server listening at http://localhost:${PORT}`);

  if (fs.existsSync(FRAMES_DIR)) {
    fs.rmSync(FRAMES_DIR, { recursive: true, force: true });
  }
  fs.mkdirSync(FRAMES_DIR, { recursive: true });

  console.log('2. Launching Chrome...');
  const browser = await puppeteer.launch({
    executablePath: CHROME_PATH,
    headless: 'new',
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-gpu',
      '--window-size=1040,660',
      '--force-device-scale-factor=1'
    ],
    defaultViewport: {
      width: 1000,
      height: 620
    }
  });

  const page = await browser.newPage();
  await page.goto(`http://localhost:${PORT}/dashboard`, { waitUntil: 'networkidle0' });

  console.log('3. Capturing interactive frames...');
  let frameIndex = 0;
  async function snap(count = 1, delay = 250) {
    for (let i = 0; i < count; i++) {
      const p = path.join(FRAMES_DIR, `frame_${String(frameIndex).padStart(4, '0')}.png`);
      await page.screenshot({ path: p });
      frameIndex++;
      if (delay > 0) await sleep(delay);
    }
  }

  // Scene 1: Initial Live Stream feed
  await snap(6, 200);

  // Scene 2: Click "+ Order Insert"
  const btnOrder = await page.$('button[onclick*="simOrder"]');
  if (btnOrder) {
    await btnOrder.click();
    await snap(4, 250);
  }

  // Scene 3: Switch to "Schema Catalog & Evolution"
  const tabSchema = await page.$('button[data-tab="schemas"]');
  if (tabSchema) {
    await tabSchema.click();
    await snap(7, 250);
  }

  // Scene 4: Switch to "Dead Letter Queue"
  const tabDlq = await page.$('button[data-tab="dlq"]');
  if (tabDlq) {
    await tabDlq.click();
    await snap(5, 250);
  }

  // Scene 5: Switch to "Rule Sandbox & Masking"
  const tabSandbox = await page.$('button[data-tab="sandbox"]');
  if (tabSandbox) {
    await tabSandbox.click();
    await snap(6, 250);
  }

  // Scene 6: Back to "Live Stream" and trigger Burst (50 EPS)
  const tabStream = await page.$('button[data-tab="stream"]');
  if (tabStream) {
    await tabStream.click();
    await sleep(200);
    const btnBurst = await page.$('button[onclick*="simBurst"]');
    if (btnBurst) {
      await btnBurst.click();
      await snap(8, 200);
    }
  }

  console.log(`Captured ${frameIndex} frames. Closing browser and server...`);
  await browser.close();
  server.close();

  console.log('4. Compiling GIF with FFmpeg...');
  // High quality 2-pass palette optimization with lanczos scale
  const ffmpegCmd = `ffmpeg -y -framerate 6 -i "${FRAMES_DIR}/frame_%04d.png" -vf "fps=6,scale=880:-1:flags=lanczos,split[s0][s1];[s0]palettegen=max_colors=128[p];[s1][p]paletteuse=dither=bayer:bayer_scale=3" "${OUTPUT_GIF}"`;
  execSync(ffmpegCmd, { stdio: 'inherit' });

  const stats = fs.statSync(OUTPUT_GIF);
  console.log(`\n🎉 Success! GIF generated at: ${OUTPUT_GIF}`);
  console.log(`File size: ${(stats.size / 1024).toFixed(1)} KB`);

  // Cleanup temp frames
  fs.rmSync(FRAMES_DIR, { recursive: true, force: true });
}

run().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
