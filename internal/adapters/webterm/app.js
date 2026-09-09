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
  const coarsePointer = matchMedia('(pointer: coarse)');
  let selecting = coarsePointer.matches;
  const selectButton = document.querySelector('#select-mode');
  function setSelecting(value) {
    selecting = value;
    selectButton.setAttribute('aria-pressed', String(value));
    if (terminal) terminal.setMouseCapture(mouse && !selecting);
    if (value) root.querySelector('textarea')?.blur();
  }
  setSelecting(selecting);
  selectButton.addEventListener('click', () => setSelecting(!selecting));
  document.querySelector('#keyboard').addEventListener('click', () => {
    setSelecting(false);
    terminal?.focus();
  });
  function fitVisualViewport() {
    const viewport = window.visualViewport;
    // Pinch zoom must not resize the PTY or fight the browser's pan gesture.
    if (!viewport || Math.abs(viewport.scale - 1) > 0.01) return;
    document.body.style.setProperty('--app-height', `${viewport.height}px`);
    window.scrollTo(0, 0);
    fit();
  }
  window.visualViewport?.addEventListener('resize', fitVisualViewport);
  fitVisualViewport();

  function syncStatus() {
    const text = status.textContent;
    const indicator = document.querySelector('#connection');
    indicator.title = text;
    indicator.dataset.state = text === 'Connected' ? 'connected' : text === 'Connecting…' ? 'connecting' : text.startsWith('Disconnected') ? 'disconnected' : 'error';
  }
  new MutationObserver(syncStatus).observe(status, { childList: true, characterData: true, subtree: true });
  function setViewCount(count = null) {
    document.querySelector('#view-count').textContent = count === null ? '—' : String(count);
    document.querySelector('#view-description').textContent = `Connected browser terminal views: ${count === null ? 'unknown' : count}`;
  }
  let counting = false;
  async function refreshViews() {
    if (counting) return;
    if (!socket || socket.readyState !== WebSocket.OPEN) {
      setViewCount();
      return;
    }
    counting = true;
    const current = socket;
    try {
      const response = await fetch('/views', { signal: AbortSignal.timeout(3000) });
      if (!response.ok) throw new Error('unavailable');
      const { active } = await response.json();
      if (!Number.isInteger(active) || active < 0) throw new Error('invalid count');
      if (socket === current && current.readyState === WebSocket.OPEN) setViewCount(active);
    } catch {
      if (socket === current) setViewCount();
    } finally { counting = false; }
  }
  setInterval(refreshViews, 2000);

  function send(event) {
    if (!socket || socket.readyState !== WebSocket.OPEN) return;
    if (socket.bufferedAmount > 1 << 20) {
      status.textContent = 'Connection too slow — input stopped. Reconnect to continue.';
      socket.close();
      return;
    }
    socket.send(JSON.stringify({ ...event, schemaVersion: VevTerminal.schemaVersion }));
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
        if (event.type === 'pointer') {
          const capture = mouse && !selecting && !event.shift;
          return { emit: capture, preventDefault: capture };
        }
        if (event.type === 'wheel') {
          // Shift+wheel is fast scroll (x10 server-side); Shift+pointer stays
          // reserved for native selection.
          const capture = mouse && !selecting;
          return { emit: capture, preventDefault: capture };
        }
        // Ctrl+Shift+F6 releases terminal focus for keyboard navigation.
        if (event.type === 'key' && event.key === 'F6' && event.ctrl && event.shift) {
          document.querySelector('#palette').focus();
          return { emit: false, preventDefault: true };
        }
        // Preserve native copy/paste and browser/OS shortcuts.
        if (event.type === 'key' && (event.meta || (event.ctrl && event.shift && ['C', 'V', 'c', 'v'].includes(event.key)) || (event.ctrl && !event.alt && event.key.toLowerCase() === 'v'))) {
          return { emit: false, preventDefault: false };
        }
        return { emit: true, preventDefault: true };
      },
      send
    });
    socket = new WebSocket(`${location.protocol === 'https:' ? 'wss:' : 'ws:'}//${location.host}/ws`);
    status.textContent = 'Connecting…';
    reconnect.hidden = true;
    // Presentation coalescing: at most one scheduled present task per
    // animation frame, preserving every incremental update in order. Each
    // queued delta is still applied in order; never drops intermediate
    // deltas. requestAnimationFrame is throttled to zero in hidden pages,
    // so a timeout fallback keeps the terminal live there.
    let queuedUpdates = [];
    let presentScheduled = false;
    function flushUpdates() {
      presentScheduled = false;
      const batch = queuedUpdates;
      queuedUpdates = [];
      try {
        for (const update of batch) terminal.apply(update);
      } catch {
        status.textContent = 'Invalid terminal update — connection stopped.';
        socket.close();
      }
    }
    function scheduleFlush() {
      if (presentScheduled) return;
      presentScheduled = true;
      if (document.visibilityState === 'visible') requestAnimationFrame(flushUpdates);
      else setTimeout(flushUpdates, 0);
    }
    socket.addEventListener('message', event => {
      try {
        const message = JSON.parse(event.data);
        if (!message || typeof message.update !== 'object' || message.update === null) throw new Error('bad update');
        queuedUpdates.push(message.update);
        scheduleFlush();
        if (mouse !== message.mouse) {
          mouse = message.mouse;
          terminal.setMouseCapture(mouse && !selecting);
        }
        if (!received) { received = true; if (!coarsePointer.matches) terminal.focus(); fit(); refreshViews(); }
        if (status.textContent !== 'Connected') status.textContent = 'Connected';
      } catch {
        status.textContent = 'Invalid terminal update — connection stopped.';
        socket.close();
      }
    });
    socket.addEventListener('close', () => {
      if (status.textContent === 'Connected' || status.textContent === 'Connecting…') {
        status.textContent = 'Disconnected — session shells remain in the daemon.';
      }
      setViewCount();
      reconnect.hidden = false;
    });
    socket.addEventListener('error', () => { status.textContent = 'Connection failed.'; });
  }

  root.addEventListener('pointerdown', event => { if (!event.shiftKey && !selecting && terminal) terminal.focus(); });
  document.querySelector('#palette').addEventListener('click', () => {
    if (!terminal) return;
    send({ type: 'key', key: ' ', code: 'Space', alt: true, ctrl: false, meta: false, shift: false, repeat: false, location: 0 });
    terminal.focus();
  });
  reconnect.addEventListener('click', start);
  new ResizeObserver(fit).observe(workspace);
  document.fonts.ready.then(fit);
  window.addEventListener('pagehide', () => { if (socket) socket.close(); });

  async function start() {
    const token = new URLSearchParams(location.hash.slice(1)).get('token');
    try {
      if (token) {
        const login = await fetch('/login', { method: 'POST', body: new URLSearchParams({ token }), credentials: 'same-origin' });
        if (!login.ok) throw new Error('login');
      }
      const health = await fetch('/health', { credentials: 'same-origin' });
      if (!health.ok) throw new Error('login');
      history.replaceState(null, '', '/');
      document.querySelector('#help').hidden = true;
      connect();
    } catch (error) {
      if (error?.message === 'login') {
        status.textContent = 'Authentication required';
        reconnect.hidden = true;
        document.querySelector('#help').hidden = false;
        return;
      }
      status.textContent = 'Disconnected — gateway unreachable. Reconnect to retry.';
      reconnect.hidden = false;
    }
  }
  window.addEventListener('hashchange', () => {
    if (new URLSearchParams(location.hash.slice(1)).has('token')) start();
  });
  start();
})();
