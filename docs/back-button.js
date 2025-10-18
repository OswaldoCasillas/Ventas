<script>
// back-button.js — Botón flotante "Volver al menú"
(() => {
  const link = document.createElement('a');
  link.href = './index.html';
  link.textContent = '⬅ Volver al menú';
  link.setAttribute('aria-label', 'Volver al menú principal');
  Object.assign(link.style, {
    position: 'fixed',
    top: '16px',
    left: '16px',
    padding: '10px 14px',
    background: '#111',
    color: '#fff',
    textDecoration: 'none',
    borderRadius: '10px',
    border: '1px solid #111',
    fontFamily: 'system-ui,Segoe UI,Roboto,Helvetica,Arial,sans-serif',
    fontSize: '14px',
    lineHeight: '1',
    zIndex: '9999',
    boxShadow: '0 2px 10px rgba(0,0,0,.15)',
    opacity: '0.92'
  });
  link.addEventListener('mouseenter', () => link.style.opacity = '1');
  link.addEventListener('mouseleave', () => link.style.opacity = '0.92');

  // Evita taparlo si el sitio ya tiene header fijo alto
  const headerLike = document.querySelector('header, nav');
  if (headerLike) link.style.top = '64px';

  document.addEventListener('DOMContentLoaded', () => {
    document.body.appendChild(link);
  });
})();
</script>
