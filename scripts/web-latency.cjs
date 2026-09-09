// Isolated gateway only; creates a retained ephemeral session per viewport.
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const fs = require('node:fs');
(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM_PATH });
  try {
    for (const viewport of [{width:800,height:500}, {width:1600,height:1000}, {width:2560,height:1440}, {width:3840,height:2160}]) {
      const page = await browser.newPage({viewport});
      if (process.env.WEB_APP_JS) await page.route('**/app.js', route => route.fulfill({path:process.env.WEB_APP_JS,contentType:'text/javascript'}));
      if (process.env.WEB_RUNTIME_JS) await page.route('**/terminal.js', route => route.fulfill({path:process.env.WEB_RUNTIME_JS,contentType:'text/javascript'}));
      await page.addInitScript(() => {
        const NativeWebSocket = window.WebSocket;
        window.wire = [];
        window.WebSocket = class extends NativeWebSocket {
          constructor(...args) {
            super(...args);
            this.addEventListener('message', event => {
              const entry = {received:performance.now(), bytes:event.data.length};
              window.wire.push(entry);
              queueMicrotask(() => { entry.applied = performance.now(); });
            });
          }
        };
      });
      await page.goto(`http://127.0.0.1:8778/#token=${fs.readFileSync(process.env.WEB_TOKEN_FILE,'utf8').trim()}`);
      await page.waitForFunction(() => document.querySelector('#status').textContent === 'Connected');
      await page.waitForTimeout(1000);
      if (process.env.WEB_LAYER) await page.evaluate(() => {
        for (const rule of ['#terminal .vev-terminal__cell { overflow: visible; }', '#terminal .vev-terminal__row { transform: translateZ(0); }']) document.styleSheets[0].insertRule(rule, document.styleSheets[0].cssRules.length);
      });
      const cdp = await page.context().newCDPSession(page);
      await cdp.send('Performance.enable');
      const trace = [];
      if (process.env.WEB_PROFILE && viewport.width === 3840) {
        cdp.on('Tracing.dataCollected', ({value}) => trace.push(...value));
        await cdp.send('Tracing.start', {categories:'devtools.timeline', transferMode:'ReportEvents'});
        await cdp.send('Profiler.enable');
        await cdp.send('Profiler.start');
      }
      const metricsBefore = await cdp.send('Performance.getMetrics');
      const samples = [];
      let expected = '';
      for (const key of 'abcdefghij') {
        expected += key;
        await page.evaluate(({key, expected}) => {
          window.sample = new Promise(resolve => {
            const output = document.querySelector('.vev-terminal__accessible-output');
            const before = output.textContent;
            const start = performance.now();
            const observer = new MutationObserver(() => {
              if (output.textContent !== before && output.textContent.includes(expected)) {
                observer.disconnect();
                const last = window.wire.at(-1);
                resolve({total:performance.now()-start, receive:last.received-start, apply:performance.now()-last.received, bytes:last.bytes});
              }
            });
            observer.observe(output, {childList:true,subtree:true,characterData:true});
            document.querySelector('textarea').dispatchEvent(new InputEvent('beforeinput', {data:key,inputType:'insertText',bubbles:true,cancelable:true}));
          });
        }, {key, expected});
        samples.push(await page.evaluate(() => Promise.race([window.sample, new Promise((_, reject) => setTimeout(() => reject(new Error('input echo timeout')), 5000))])));
      }
      if (process.env.WEB_PROFILE && viewport.width === 3840) {
        const completed = new Promise(resolve => cdp.once('Tracing.tracingComplete', resolve));
        await cdp.send('Tracing.end');
        await completed;
        const durations = {};
        for (const event of trace) if (event.ph === 'X' && event.dur) durations[event.name] = (durations[event.name] || 0) + event.dur / 1000;
        console.log('Trace ms', Object.entries(durations).sort((a,b)=>b[1]-a[1]).slice(0,20));
        const {profile} = await cdp.send('Profiler.stop');
        const counts = new Map();
        for (const id of profile.samples || []) counts.set(id, (counts.get(id) || 0)+1);
        console.log('CPU samples', profile.nodes.map(node => ({name:node.callFrame.functionName, url:node.callFrame.url, count:counts.get(node.id)||0})).sort((a,b)=>b.count-a.count).slice(0,15));
      }
      const metricsAfter = await cdp.send('Performance.getMetrics');
      const costs = {};
      for (const metric of metricsAfter.metrics) {
        if (['LayoutDuration','RecalcStyleDuration','ScriptDuration','TaskDuration','LayoutCount','RecalcStyleCount'].includes(metric.name)) {
          costs[metric.name] = metric.value - metricsBefore.metrics.find(before => before.name === metric.name).value;
        }
      }
      console.log(JSON.stringify({viewport, samplesMs:samples, costs}));
      await page.keyboard.press('Control+c');
      const action = async (name, text) => {
        await page.evaluate(({name, text}) => {
          window.actionSample = new Promise(resolve => {
            const output = document.querySelector('.vev-terminal__accessible-output');
            const start = performance.now();
            const observer = new MutationObserver(() => {
              if (output.textContent.includes(text)) {
                observer.disconnect();
                const last = window.wire.at(-1);
                resolve({total:performance.now()-start,receive:last.received-start,apply:performance.now()-last.received,bytes:last.bytes});
              }
            });
            observer.observe(output, {childList:true,subtree:true});
            if (name === 'palette') document.querySelector('#palette').click();
            else document.querySelector('textarea').dispatchEvent(new KeyboardEvent('keydown', {key:'Enter',code:'Enter',bubbles:true,cancelable:true}));
          });
        }, {name, text});
        const ms = await page.evaluate(() => Promise.race([window.actionSample, new Promise((_, reject) => setTimeout(() => reject(new Error('action timeout')), 5000))]));
        console.log(JSON.stringify({viewport, action:name, ms}));
      };
      await action('palette', 'Commands');
      await page.keyboard.type('SPR');
      await page.waitForTimeout(300);
      await action('split', '│');
      await page.close();
    }
  } finally { await browser.close(); }
})().catch(error => { console.error(error); process.exitCode=1; });
