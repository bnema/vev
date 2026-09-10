// Run against an isolated gateway. Supply PLAYWRIGHT_MODULE, VEV_BINARY and VEV_ENV.
// The test creates sessions/panes; it never targets an existing named session.
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const { execFileSync } = require('node:child_process');
const assert = require('node:assert/strict');

(async () => {
  if (!process.env.VEV_BINARY || !process.env.VEV_ENV) throw new Error('VEV_BINARY and isolated VEV_ENV are required');
  const browser = await chromium.launch({
    executablePath: process.env.CHROMIUM_PATH || undefined,
    headless: true
  });
  try {
    const page = await browser.newPage({ viewport: { width: 1280, height: 800 } });
    const errors = [];
    page.on('pageerror', error => errors.push(error.message));
    const output = execFileSync(process.env.VEV_BINARY, ['--web-daemon'], { encoding: 'utf8' });
    const token = output.match(/#token=([A-Za-z0-9_-]+)/)[1];
    await page.goto(`http://127.0.0.1:8778/#token=${token}`);
    const connected = () => page.waitForFunction(() => document.querySelector('#status').textContent.startsWith('Connected'));
    const contains = text => page.waitForFunction(text => document.querySelector('.vev-terminal__accessible-output').textContent.includes(text), text);
    await connected();
    assert.equal(new URL(page.url()).hash, '');
    await page.keyboard.type("printf '\\033[2J\\033[HDIMCHECK\\n'");
    await page.keyboard.press('Enter');
    await contains('DIMCHECK');
    const markerColor = () => page.evaluate(() => {
      const cell = [...document.querySelectorAll('.vev-terminal__cell')].find(cell => cell.textContent === 'D');
      return cell && getComputedStyle(cell).color;
    });
    const normalColor = await markerColor();
    assert.ok(normalColor);
    await page.locator('#palette').click();
    await contains('Commands');
    assert.notEqual(await markerColor(), normalColor, 'palette backdrop uses theme dimming');
    await page.keyboard.press('Escape');
    await page.waitForFunction(() => !document.querySelector('.vev-terminal__accessible-output').textContent.includes('Commands'));
    assert.equal(await markerColor(), normalColor, 'closing the palette restores the theme');
    await page.locator('#palette').click();
    await contains('Commands');
    await page.keyboard.type('SPR');
    await page.keyboard.press('Enter');
    await contains('│');
    await page.keyboard.type("printf '\\033[2J\\033[HRIGHT-PANE\\n'");
    await page.keyboard.press('Enter');
    await contains('RIGHT-PANE');
    await page.keyboard.press('Alt+h');
    await page.keyboard.type("printf '\\033[2J\\033[HLEFT-PANE\\n'");
    await page.keyboard.press('Enter');
    await contains('LEFT-PANE');
    const bounds = await page.locator('#terminal').boundingBox();
    await page.mouse.click(bounds.x + bounds.width * .75, bounds.y + 90);
    await page.keyboard.type('MOUSE-CHECK');
    await contains('MOUSE-CHECK');
    await page.keyboard.press('Control+c');
    await page.setViewportSize({ width: 960, height: 600 });
    await page.waitForFunction(() => document.querySelectorAll('.vev-terminal__row').length < 40);
    await page.locator('#terminal').evaluate(root => {
      const clipboardData = new DataTransfer();
      clipboardData.setData('text/plain', 'paste');
      root.querySelector('textarea').dispatchEvent(new ClipboardEvent('paste', { clipboardData, bubbles: true, cancelable: true }));
    });
    await page.waitForFunction(() => {
      const rows = [...document.querySelectorAll('.vev-terminal__row')];
      const right = rows.map(row => {
        const cells = [...row.children];
        const divider = cells.findIndex(cell => cell.textContent === '│');
        return cells.slice(divider + 1).map(cell => cell.textContent).join('').trimEnd();
      }).join('');
      return right.includes('paste');
    });
    await page.keyboard.press('Control+c');
    await page.reload();
    await connected();
    assert.deepEqual(errors, []);
    console.log('Web visual smoke passed: auth, palette, split, keyboard, mouse input, resize, paste, reload.');
  } finally {
    await browser.close();
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
