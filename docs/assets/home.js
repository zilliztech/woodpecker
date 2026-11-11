(() => {
  'use strict';
  const menu = document.querySelector('.menu-toggle');
  const nav = document.querySelector('#site-nav');
  menu.addEventListener('click', () => {
    const open = nav.classList.toggle('open');
    menu.setAttribute('aria-expanded', String(open));
    menu.setAttribute('aria-label', open ? 'Close navigation' : 'Open navigation');
  });
  nav.querySelectorAll('a').forEach(link => link.addEventListener('click', () => {
    nav.classList.remove('open');
    menu.setAttribute('aria-expanded', 'false');
    menu.setAttribute('aria-label', 'Open navigation');
  }));
  const flows = {
    write: ['01', 'Replicate, then acknowledge.', ' The client writes to the selected quorum. Acknowledgement follows the configured durability policy; compaction uploads completed data to object storage.'],
    read: ['02', 'Read where the data lives.', ' Active data is served by LogStore replicas. Compacted data is read from shared object storage; multiple nodes serving it are not independent copies.'],
    recovery: ['03', 'Recover from available evidence.', ' Use metadata and replica state to establish the recoverable boundary. Progress depends on available data and quorum; inspect a blocked segment before choosing a repair.']
  };
  const engine = document.querySelector('#engine');
  const panel = document.querySelector('#flow-panel');
  const tabs = [...document.querySelectorAll('[role="tab"]')];
  function selectTab(tab) {
    const flow = tab.dataset.flow;
    engine.dataset.flow = flow;
    tabs.forEach(item => {
      item.setAttribute('aria-selected', String(item === tab));
      item.tabIndex = item === tab ? 0 : -1;
    });
    panel.setAttribute('aria-labelledby', tab.id);
    panel.querySelector('.step-number').textContent = flows[flow][0];
    panel.querySelector('strong').textContent = flows[flow][1];
    panel.querySelector('p > span').textContent = flows[flow][2];
    engine.querySelector('[data-node="2"] .replica-note').textContent = flow === 'recovery' ? 'Unavailable' : 'Local WAL';
    engine.querySelector('.path-label').textContent = flow === 'read' ? 'SHARED SEGMENTS' : flow === 'recovery' ? 'VERIFY COVERAGE' : 'COMPACT & UPLOAD';
  }
  tabs.forEach((tab, index) => {
    tab.addEventListener('click', () => selectTab(tab));
    tab.addEventListener('keydown', event => {
      let next;
      if (event.key === 'ArrowRight') next = (index + 1) % tabs.length;
      if (event.key === 'ArrowLeft') next = (index + tabs.length - 1) % tabs.length;
      if (event.key === 'Home') next = 0;
      if (event.key === 'End') next = tabs.length - 1;
      if (next !== undefined) {
        event.preventDefault();
        selectTab(tabs[next]);
        tabs[next].focus();
      }
    });
  });
})();
