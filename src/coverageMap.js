const usaMap = require('./usaMap');
const { CNV_COLOR_SCALE, cnvColorMeta } = require('./cnvColors');
const { buildCnvLookup, COVERAGE_REGIONS } = require('./stateNames');
const { buildStateNameLabelsSvg, MAP_VIEWBOX_EXPANDED } = require('./mapStateLabels');

function htmlEscape(s) {
  return String(s ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function formatMapDate(d = new Date()) {
  return d
    .toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' })
    .toUpperCase();
}

function buildCoverageMapSvg(cnvByAbbr) {
  const paths = usaMap.locations
    .map(loc => {
      const cnv = cnvByAbbr.has(loc.id) ? cnvByAbbr.get(loc.id) : 0;
      const meta = cnvColorMeta(cnv);
      return `<path id="${loc.id}" class="cov-state" fill="${meta.color}" stroke="#1a1a1a" stroke-width="0.6" data-cnv="${htmlEscape(meta.key)}" d="${loc.path}">
        <title>${htmlEscape(loc.name)} — CNV ${htmlEscape(meta.key)}</title>
      </path>`;
    })
    .join('\n');

  const labels = buildStateNameLabelsSvg(htmlEscape, usaMap.locations, cnvByAbbr);

  return `<svg class="cov-map-svg" viewBox="${MAP_VIEWBOX_EXPANDED}" xmlns="http://www.w3.org/2000/svg" aria-label="US coverage map" overflow="visible">
    ${paths}
    ${labels}
  </svg>`;
}

function countByCnvKey(cnvByAbbr) {
  const counts = { '1': 0, '0.5': 0, '0.33': 0, '0': 0, other: 0 };
  for (const loc of usaMap.locations) {
    const cnv = cnvByAbbr.has(loc.id) ? cnvByAbbr.get(loc.id) : 0;
    const key = cnvColorMeta(cnv).key;
    counts[key] = (counts[key] || 0) + 1;
  }
  return counts;
}

function buildPlomoRegionsHtml(cnvByAbbr) {
  const plomoAbbrs = new Set(
    usaMap.locations
      .filter(loc => {
        const cnv = cnvByAbbr.has(loc.id) ? cnvByAbbr.get(loc.id) : 0;
        return cnvColorMeta(cnv).value === 0;
      })
      .map(loc => loc.id)
  );

  if (!plomoAbbrs.size) {
    return '<div class="cov-region-empty">No states with CNV 0.</div>';
  }

  const nameByAbbr = Object.fromEntries(usaMap.locations.map(l => [l.id, l.name]));

  return COVERAGE_REGIONS.map(region => {
    const names = region.abbrs.filter(ab => plomoAbbrs.has(ab)).map(ab => nameByAbbr[ab]);
    if (!names.length) return '';
    return `<div class="cov-region">
      <div class="cov-region-title">${htmlEscape(region.label)} (${names.length})</div>
      <div class="cov-region-states">${htmlEscape(names.join(', '))}</div>
    </div>`;
  })
    .filter(Boolean)
    .join('');
}

function buildCoverageLegendHtml() {
  return CNV_COLOR_SCALE.map(
    e => `<div class="cov-legend-item">
      <span class="cov-legend-swatch" style="background:${e.color}"></span>
      <span>${htmlEscape(e.label)}</span>
    </div>`
  ).join('');
}

/** HTML del mapa de cobertura (página aparte en el PDF). */
function buildCoverageMapHtml(statesRows, asOfDate = new Date(), dataWarning = '') {
  const rows = statesRows || [];
  const cnvByAbbr = buildCnvLookup(rows);
  const counts = countByCnvKey(cnvByAbbr);
  const activeCount = counts['1'] || 0;
  const plomoCount = counts['0'] || 0;
  const mapSvg = buildCoverageMapSvg(cnvByAbbr);
  const regionsHtml = buildPlomoRegionsHtml(cnvByAbbr);
  const asOf = formatMapDate(asOfDate);
  const warningHtml = dataWarning
    ? `<div class="cov-map-error">${htmlEscape(dataWarning)}</div>`
    : '';

  return `
  <div class="cov-map-wrap">
    ${warningHtml}
    <div class="cov-map-header">
      <div class="cov-map-brand">1-800-NO-FAULT</div>
      <div class="cov-map-title">COVERAGE MAP</div>
      <div class="cov-map-date">AS OF ${htmlEscape(asOf)}</div>
    </div>

    <div class="cov-map-body">
      <div class="cov-map-canvas">${mapSvg}</div>
      <div class="cov-map-sidebar">
        <div class="cov-legend-title">CNV Legend</div>
        <div class="cov-legend">${buildCoverageLegendHtml()}</div>
        <div class="cov-map-stats">
          <div><strong>${activeCount}</strong> turquesa (CNV 1)</div>
          <div><strong>${counts['0.5'] || 0}</strong> naranja (0.5)</div>
          <div><strong>${counts['0.33'] || 0}</strong> rojo (0.33)</div>
          <div><strong>${plomoCount}</strong> plomo (0)</div>
        </div>
        <div class="cov-map-not-covered">
          <div class="cov-footer-title">States with CNV 0 (not covered)</div>
          <div class="cov-regions">${regionsHtml}</div>
        </div>
      </div>
    </div>
  </div>`;
}

module.exports = { buildCoverageMapHtml, buildCoverageMapSvg };
