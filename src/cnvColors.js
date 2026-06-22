/** Escala CNV → color. 1 Turquoise, 0.5 Blue Green, 0.33 Orange, 0 Gray. */
const CNV_COLOR_SCALE = [
  { value: 1, color: '#00CEC8', textColor: '#1a1a1a', label: '1 — Turquoise (active)' },
  { value: 0.5, color: '#0f7d80', textColor: '#eef4f7', label: '0.5 — Blue Green' },
  { value: 0.33, color: '#f8d39c', textColor: '#1a1a1a', label: '0.33 — Orange' },
  { value: 0, color: '#9e9e9e', textColor: '#1a1a1a', label: '0 — Gray (no coverage)' },
];

function parseCnvNumeric(v) {
  if (v === null || v === undefined || String(v).trim() === '') return null;
  if (typeof v === 'number') return Number.isFinite(v) ? v : null;
  const n = parseFloat(String(v).replace(/[$,]/g, '').trim());
  return Number.isNaN(n) ? null : n;
}

function cnvColorMeta(v) {
  const n = parseCnvNumeric(v);
  if (n === null) {
    return { color: '#9e9e9e', textColor: '#1a1a1a', key: '0', label: '0 — Plomo', value: 0 };
  }
  const tol = 0.001;
  for (const entry of CNV_COLOR_SCALE) {
    if (Math.abs(n - entry.value) < tol) {
      return {
        color: entry.color,
        textColor: entry.textColor,
        key: String(entry.value),
        label: entry.label,
        value: entry.value,
      };
    }
  }
  return {
    color: '#d0d0d0',
    textColor: '#1a1a1a',
    key: 'other',
    label: String(v),
    value: n,
  };
}

module.exports = { CNV_COLOR_SCALE, parseCnvNumeric, cnvColorMeta };
