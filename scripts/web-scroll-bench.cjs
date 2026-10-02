// Browser scroll benchmark for the web terminal. Starts an isolated gateway,
// fills history, scrolls it fast with the wheel and reports where the
// browser's main thread spent its time. Linux only. See docs/web-terminal.md.
const { spawnSync } = require('node:child_process');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const options = { lines: 10000, notches: 120, port: 8799, viewport: '1600x1000', scale: 1, cdp: '' };
for (let i = 2; i < process.argv.length; i += 2) {
  const key = process.argv[i].replace(/^--/, '');
  if (!(key in options) || process.argv[i + 1] === undefined) {
    console.error('usage: web-scroll-bench.cjs [--lines N] [--notches N] [--port N] [--viewport WxH] [--scale F] [--cdp URL]');
    process.exit(2);
  }
  options[key] = typeof options[key] === 'number' ? Number(process.argv[i + 1]) : process.argv[i + 1];
}
if (!process.env.VEV_BINARY) {
  console.error('VEV_BINARY is required');
  process.exit(2);
}
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');

// Main-thread trace events, summed per phase.
const PHASES = {
  total: ['RunTask'],
  script: ['FunctionCall', 'EvaluateScript', 'v8.callFunction'],
  style: ['UpdateLayoutTree'],
  layout: ['Layout'],
  paint: ['Paint', 'PaintImage', 'RasterTask'],
};

// Kill every process started under root (session daemon and gateway).
function stopProcesses(root) {
  for (const pid of fs.readdirSync('/proc').filter(name => /^\d+$/.test(name))) {
    try {
      const environ = fs.readFileSync(`/proc/${pid}/environ`, 'utf8').split('\0');
      if (environ.includes(`VEV_ENV_ROOT=${root}`)) process.kill(Number(pid), 'SIGTERM');
    } catch {}
  }
}

(async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'vev-scroll-bench-'));
  const env = { ...process.env, VEV_ENV: 'scroll-bench', VEV_ENV_ROOT: root, SHELL: '/bin/bash' };
  const origin = `http://127.0.0.1:${options.port}`;
  let browser;
  try {
    const started = spawnSync(process.env.VEV_BINARY, ['--web-daemon', '--web-listen', `127.0.0.1:${options.port}`, '--web-origin', origin], { cwd: root, env, encoding: 'utf8', timeout: 20000 });
    const token = started.stdout?.match(/#token=([A-Za-z0-9_-]+)/)?.[1];
    if (!token) throw new Error(`gateway did not start: ${started.stderr || started.error}`);

    const [width, height] = options.viewport.split('x').map(Number);
    let page;
    if (options.cdp) {
      browser = await chromium.connectOverCDP(options.cdp);
      page = await browser.contexts()[0].newPage();
      await page.bringToFront();
    } else {
      browser = await chromium.launch({ executablePath: process.env.CHROMIUM_PATH });
      page = await browser.newPage({ viewport: { width, height }, deviceScaleFactor: options.scale });
    }
    if (process.env.WEB_APP_JS) await page.route('**/app.js', route => route.fulfill({ path: process.env.WEB_APP_JS, contentType: 'text/javascript' }));
    if (process.env.WEB_RUNTIME_JS) await page.route('**/terminal.js', route => route.fulfill({ path: process.env.WEB_RUNTIME_JS, contentType: 'text/javascript' }));

    // Count WebSocket frames and bytes received by the page.
    await page.addInitScript(() => {
      const NativeWebSocket = window.WebSocket;
      window.wire = { frames: 0, bytes: 0 };
      window.WebSocket = class extends NativeWebSocket {
        constructor(...args) {
          super(...args);
          this.addEventListener('message', event => {
            window.wire.frames += 1;
            window.wire.bytes += typeof event.data === 'string' ? event.data.length : event.data.byteLength ?? event.data.size;
          });
        }
      };
    });
    await page.goto(`${origin}/#token=${token}`);
    await page.waitForFunction(() => document.querySelector('#status').textContent === 'Connected');
    await page.waitForTimeout(1000);
    await page.locator('#terminal').click();
    // Colored, styled and wide content, close to a real TUI.
    await page.keyboard.type(`for i in $(seq 1 ${options.lines}); do printf '\\e[3%dm%05d\\e[0m │ \\e[1mlorem\\e[0m ipsum \\e[48;5;236mdolor sit\\e[0m amet ✓ consectetur … │ %s\\n' $((i%7+1)) $i "$(printf '%*s' $((i%40)) x)"; done; echo bench-$((6*7))-ready\n`, { delay: 2 });
    await page.waitForFunction(() => document.querySelector('.vev-terminal__accessible-output').textContent.includes('bench-42-ready'), null, { timeout: 120000 });
    const geometry = await page.evaluate(() => {
      const style = getComputedStyle(document.querySelector('.vev-terminal'));
      return { columns: Number(style.getPropertyValue('--vev-cols')), rows: Number(style.getPropertyValue('--vev-rows')), viewport: [innerWidth, innerHeight], scale: devicePixelRatio };
    });

    const box = await page.locator('#terminal').boundingBox();
    await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2);
    const cdp = await page.context().newCDPSession(page);
    const events = [];
    cdp.on('Tracing.dataCollected', event => events.push(...event.value));
    const done = new Promise(resolve => cdp.once('Tracing.tracingComplete', resolve));
    await cdp.send('Tracing.start', { transferMode: 'ReportEvents', traceConfig: { includedCategories: ['devtools.timeline', 'disabled-by-default-devtools.timeline', 'v8'] } });
    const wireBefore = await page.evaluate(() => ({ ...window.wire }));
    const start = Date.now();
    // Half the notches scroll up into history, half come back down.
    for (let i = 0; i < options.notches; i++) {
      await page.mouse.wheel(0, i < options.notches / 2 ? -100 : 100);
      await page.waitForTimeout(16);
    }
    await page.waitForTimeout(800);
    const wall = (Date.now() - start) / 1000;
    const wireAfter = await page.evaluate(() => ({ ...window.wire }));
    await cdp.send('Tracing.end');
    await done;

    // The page's main thread is the one that ran Layout.
    const layout = events.find(event => event.name === 'Layout' && event.ph === 'X');
    const frames = wireAfter.frames - wireBefore.frames;
    const bytes = wireAfter.bytes - wireBefore.bytes;
    const result = {
      ...geometry, notches: options.notches, wallSeconds: wall,
      wireFrames: frames, wireKB: Math.round(bytes / 1024), avgFrameKB: frames ? Number((bytes / frames / 1024).toFixed(1)) : 0
    };
    for (const [phase, names] of Object.entries(PHASES)) {
      let ms = 0;
      for (const event of events) {
        if (event.ph === 'X' && event.dur && event.pid === layout?.pid && event.tid === layout?.tid && names.includes(event.name)) ms += event.dur / 1000;
      }
      result[`${phase}Ms`] = Math.round(ms);
    }
    result.totalMsPerNotch = Number((result.totalMs / options.notches).toFixed(1));
    console.log(JSON.stringify(result, null, 2));
    await page.close();
  } finally {
    if (browser) await browser.close().catch(() => {});
    stopProcesses(root);
    fs.rmSync(root, { recursive: true, force: true });
  }
})().catch(error => {
  console.error(error);
  process.exit(1);
});
