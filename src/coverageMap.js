const fs = require('fs');
const path = require('path');
const usaMap = require('./usaMap');
const { CNV_COLOR_SCALE, cnvColorMeta } = require('./cnvColors');
const { buildCnvLookup, COVERAGE_REGIONS } = require('./stateNames');
const { buildStateNameLabelsSvg } = require('./mapStateLabels');

const MAIN_MAP_VIEWBOX = '260 0 1210 600'; // tighter crop → ~9% larger map scale (0.752 vs 0.689)
const INSET_VIEWBOX = {
  ak: '120 515 430 240',
  hi: '575 585 55 80',
};

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

function logoDataUri() {
  const logoPath = path.join(__dirname, 'assets', 'no-fault-logo.png');
  const logoBase64 = fs.readFileSync(logoPath, 'base64');
  return `data:image/png;base64,${logoBase64}`;
}

function buildStatePath(loc, cnvByAbbr, className = 'cov-state') {
  const cnv = cnvByAbbr.has(loc.id) ? cnvByAbbr.get(loc.id) : 0;
  const meta = cnvColorMeta(cnv);
  return `<path id="${loc.id}" class="${className}" fill="${meta.color}" data-cnv="${htmlEscape(meta.key)}" d="${loc.path}">
    <title>${htmlEscape(loc.name)} — CNV ${htmlEscape(meta.key)}</title>
  </path>`;
}

function buildInsetSvg(stateId, cnvByAbbr) {
  const loc = usaMap.locations.find(l => l.id === stateId);
  if (!loc) return '';
  return `<div class="cov-map-inset">
    <svg viewBox="${INSET_VIEWBOX[stateId]}" xmlns="http://www.w3.org/2000/svg" aria-label="${htmlEscape(loc.name)} inset">
      ${buildStatePath(loc, cnvByAbbr, 'cov-state cov-state-inset')}
    </svg>
    <div class="cov-map-inset-label">${htmlEscape(loc.name)}</div>
  </div>`;
}

function buildOverlayLegendHtml() {
  return `<div class="cov-map-overlay-legend">
    ${CNV_COLOR_SCALE.map(e => `
      <div class="cov-map-overlay-item">
        <span style="background:${e.color}"></span>
        <span>${htmlEscape(e.label)}</span>
      </div>`).join('')}
  </div>`;
}

function buildCoverageMapSvg(cnvByAbbr, overlayHtml = '') {
  const mainLocations = usaMap.locations.filter(loc => !['ak', 'hi'].includes(loc.id));
  const paths = mainLocations
    .map(loc => {
      return buildStatePath(loc, cnvByAbbr);
    })
    .join('\n');

  const labels = buildStateNameLabelsSvg(htmlEscape, mainLocations, cnvByAbbr);

  return `<div class="cov-map-stage">
    <svg class="cov-map-svg" viewBox="${MAIN_MAP_VIEWBOX}" xmlns="http://www.w3.org/2000/svg" aria-label="US coverage map" overflow="visible">
      ${paths}
      ${labels}
    </svg>
    <div class="cov-map-insets">
      ${buildInsetSvg('ak', cnvByAbbr)}
      ${buildInsetSvg('hi', cnvByAbbr)}
    </div>
    ${overlayHtml}
  </div>`;
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

function buildCoverageStatsHtml(counts, plomoCount) {
  return `
    <div><strong>${counts['1'] || 0}</strong> Turquoise (CNV 1)</div>
    <div><strong>${counts['0.5'] || 0}</strong> Blue Green (0.5)</div>
    <div><strong>${counts['0.33'] || 0}</strong> Orange (0.33)</div>
    <div><strong>${plomoCount}</strong> Gray (0)</div>`;
}

/** HTML del mapa de cobertura (página aparte en el PDF). */
function buildCoverageMapHtml(statesRows, asOfDate = new Date(), dataWarning = '') {
  const rows = statesRows || [];
  const cnvByAbbr = buildCnvLookup(rows);
  const counts = countByCnvKey(cnvByAbbr);
  const plomoCount = counts['0'] || 0;
  const mapSvg = buildCoverageMapSvg(cnvByAbbr, buildOverlayLegendHtml());
  const regionsHtml = buildPlomoRegionsHtml(cnvByAbbr);
  const asOf = formatMapDate(asOfDate);
  const warningHtml = dataWarning
    ? `<div class="cov-map-error">${htmlEscape(dataWarning)}</div>`
    : '';

  return `
  <div class="cov-map-wrap">
    ${warningHtml}
    <div class="cov-map-header">
      <div class="cov-map-logo">
        <img class="cov-map-logo-img" src="${logoDataUri()}" alt="1-800-NO-FAULT" />
      </div>
      <div class="cov-map-heading">
        <div class="cov-map-title">COVERAGE MAP</div>
        <div class="cov-map-date">AS OF ${htmlEscape(asOf)}</div>
        <div class="cov-map-country">🇺🇸 UNITED STATES OF AMERICA</div>
      </div>
    </div>

    <div class="cov-map-body">
      <div class="cov-map-canvas">${mapSvg}</div>
    </div>

    <div class="cov-map-footer">
      <div class="cov-footer-regions-block">
        <div class="cov-footer-title">States with CNV 0 (not covered)</div>
        <div class="cov-regions">${regionsHtml}</div>
      </div>
    </div>
  </div>`;
}

module.exports = { buildCoverageMapHtml, buildCoverageMapSvg };
