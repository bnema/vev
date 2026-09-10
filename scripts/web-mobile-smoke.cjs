// Isolated profile only. Device emulation does not emulate a real soft keyboard.
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const { execFileSync } = require('node:child_process');
const assert = require('node:assert/strict');
(async () => {
  if (!process.env.VEV_BINARY || !process.env.VEV_ENV) throw new Error('VEV_BINARY and isolated VEV_ENV are required');
  const link = execFileSync(process.env.VEV_BINARY, ['--web-daemon'], { encoding: 'utf8' }).match(/http:\/\/127\.0\.0\.1:8778\/#token=[A-Za-z0-9_-]+/)[0];
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM_PATH });
  try {
    const page = await browser.newPage({ viewport: { width: 390, height: 844 }, isMobile: true, hasTouch: true });
    await page.goto(link);
    await page.waitForFunction(() => document.querySelector('#status').textContent === 'Connected');
    assert.equal(await page.locator('#select-mode').getAttribute('aria-pressed'), 'true');
    assert.equal(await page.evaluate(() => document.activeElement.tagName === 'TEXTAREA'), false);
    for (const id of ['keyboard', 'select-mode', 'palette']) {
      const bounds = await page.locator('#' + id).boundingBox();
      assert.ok(bounds.width >= 44 && bounds.height >= 44);
    }
    await page.locator('#keyboard').tap();
    assert.equal(await page.evaluate(() => document.activeElement.tagName), 'TEXTAREA');
    assert.equal(await page.locator('#select-mode').getAttribute('aria-pressed'), 'false');
    await page.locator('#select-mode').tap();
    assert.equal(await page.evaluate(() => document.activeElement.tagName === 'TEXTAREA'), false);
    await page.setViewportSize({ width: 390, height: 400 });
    await page.waitForFunction(() => document.querySelector('#workspace').getBoundingClientRect().bottom <= window.innerHeight + 1);
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth), true);
    console.log('Mobile emulation passed: touch targets, explicit keyboard, selection mode, compact viewport.');
  } finally { await browser.close(); }
})().catch(error => { console.error(error); process.exitCode = 1; });
