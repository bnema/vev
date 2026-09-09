(() => {
  'use strict';
  const root = document.querySelector('#terminal');
  const workspace = document.querySelector('#workspace');
  const status = document.querySelector('#status');
  const reconnect = document.querySelector('#reconnect');
  let socket;
  let terminal;
  let mouse = false;
  let lastGeometry = '';
  let fitting = 0;
  let received = false;

  function send(event) {
    if (!socket || socket.readyState !== WebSocket.OPEN) return;
    if (socket.bufferedAmount > 1 << 20) {
      status.textContent = 'Connection too slow — input stopped. Reconnect to continue.';
      socket.close();
      return;
    }
    socket.send(JSON.stringify({ ...event, schemaVersion: 1 }));
  }

  function fit() {
    if (fitting || !received) return;
    fitting = requestAnimationFrame(() => {
      fitting = 0;
      const cell = root.querySelector('.vev-terminal__cell');
      const row = root.querySelector('.vev-terminal__row');
      if (!cell || !row) return;
      const style = getComputedStyle(row);
      const cellWidth = parseFloat(style.gridTemplateColumns);
      const cellHeight = row.getBoundingClientRect().height;
      if (!(cellWidth > 0 && cellHeight > 0)) return;
      const columns = Math.max(2, Math.min(512, Math.floor((workspace.clientWidth - 8) / cellWidth)));
      const rows = Math.max(2, Math.min(256, Math.floor((workspace.clientHeight - 8) / cellHeight)));
      const event = { type: 'resize', columns, rows, pixelWidth: columns * cellWidth, pixelHeight: rows * cellHeight, cellWidth, cellHeight, devicePixelRatio: window.devicePixelRatio };
      const identity = JSON.stringify(event);
      if (identity !== lastGeometry) { lastGeometry = identity; send(event); }
    });
  }

  function connect() {
    if (socket && socket.readyState < WebSocket.CLOSING) return;
    if (terminal) terminal.destroy();
    received = false;
    mouse = false;
    lastGeometry = '';
    terminal = VevTerminal.mount(root, {
      label: 'vev terminal multiplexer',
      decide(event) {
        if (event.type === 'resize' || event.type === 'focus') return { emit: false, preventDefault: false };
        if (event.type === 'pointer' || event.type === 'wheel') {
          const capture = mouse && !event.shift;
          return { emit: capture, preventDefault: capture };
        }
        // Preserve native copy/paste and browser/OS shortcuts.
        if (event.type === 'key' && (event.meta || (event.ctrl && event.shift && ['C', 'V', 'c', 'v'].includes(event.key)) || (event.ctrl && !event.alt && event.key.toLowerCase() === 'v'))) {
          return { emit: false, preventDefault: false };
        }
        return { emit: true, preventDefault: true };
      },
      send
    });
    socket = new WebSocket(`ws://${location.host}/ws`);
    status.textContent = 'Connecting…';
    reconnect.hidden = true;
    socket.addEventListener('message', event => {
      try {
        const message = JSON.parse(event.data);
        terminal.apply(message.update);
        if (mouse !== message.mouse) {
          mouse = message.mouse;
          terminal.setMouseCapture(mouse);
        }
        if (!received) { received = true; terminal.focus(); fit(); }
        status.textContent = 'Connected';
      } catch {
        status.textContent = 'Invalid terminal update — connection stopped.';
        socket.close();
      }
    });
    socket.addEventListener('close', () => {
      status.textContent = 'Disconnected — session shells remain in the daemon.';
      reconnect.hidden = false;
    });
    socket.addEventListener('error', () => { status.textContent = 'Connection failed.'; });
  }

  root.addEventListener('pointerdown', event => { if (!event.shift && terminal) terminal.focus(); });
  document.querySelector('#palette').addEventListener('click', () => {
    send({ type: 'key', key: ' ', code: 'Space', alt: true, ctrl: false, meta: false, shift: false, repeat: false, location: 0 });
    terminal.focus();
  });
  reconnect.addEventListener('click', connect);
  new ResizeObserver(fit).observe(workspace);
  document.fonts.ready.then(fit);
  window.addEventListener('pagehide', () => { if (socket) socket.close(); });

  async function start() {
    const token = new URLSearchParams(location.hash.slice(1)).get('token');
    history.replaceState(null, '', '/');
    try {
      if (token) {
        const login = await fetch('/login', { method: 'POST', body: new URLSearchParams({ token }), credentials: 'same-origin' });
        if (!login.ok) throw new Error('login');
      }
      const health = await fetch('/health', { credentials: 'same-origin' });
      if (!health.ok) throw new Error('login');
      connect();
    } catch {
      status.textContent = 'Authentication required';
      document.querySelector('#help').hidden = false;
    }
  }
  start();
})();
