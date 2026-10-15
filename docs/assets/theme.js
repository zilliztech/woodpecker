/* Set the theme before first paint; keep all pages/tabs on the same preference. */
(() => {
  'use strict';
  const root = document.documentElement;
  const icons = '<svg class="wp-icon-sun" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" aria-hidden="true"><circle cx="12" cy="12" r="4"></circle><path d="M12 2v2m0 16v2M2 12h2m16 0h2M5 5l1.5 1.5m11 11L19 19M5 19l1.5-1.5m11-11L19 5"></path></svg><svg class="wp-icon-moon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" aria-hidden="true"><path d="M20.7 13.4A8.8 8.8 0 0 1 10.6 3.3a9 9 0 1 0 10.1 10.1Z"></path></svg>';
  function apply(value) {
    const light = value === 'light';
    root.classList.toggle('light', light);
    root.classList.toggle('dark', !light);
    document.querySelectorAll('[data-theme-toggle]').forEach(button => {
      const label = light ? 'Switch to dark theme' : 'Switch to light theme';
      button.setAttribute('aria-label', label);
      button.setAttribute('title', label);
    });
    document.querySelector('meta[name="theme-color"]')?.setAttribute('content', light ? '#f7f9ff' : '#080d1a');
  }
  let saved;
  try { saved = localStorage.getItem('wp-theme'); } catch (_) {}
  apply(saved);
  function initializeControls() {
    document.querySelectorAll('[data-theme-toggle]').forEach(button => {
      button.innerHTML = icons;
      button.addEventListener('click', () => {
        const theme = root.classList.contains('light') ? 'dark' : 'light';
        try { localStorage.setItem('wp-theme', theme); } catch (_) {}
        apply(theme);
      });
    });
    apply(root.classList.contains('light') ? 'light' : 'dark');
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', initializeControls, {once: true});
  else initializeControls();
  // Restore the preference when navigating back through the browser cache.
  window.addEventListener('pageshow', event => {
    if (!event.persisted) return;
    try { apply(localStorage.getItem('wp-theme')); } catch (_) {}
  });
  window.addEventListener('storage', event => {
    if (event.key === 'wp-theme' || event.key === null) apply(event.newValue);
  });
})();
