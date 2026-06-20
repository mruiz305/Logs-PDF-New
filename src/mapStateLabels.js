const usaMap = require('./usaMap');
const { buildLabelPointsFromLocations } = require('./mapCentroids');
const { cnvColorMeta } = require('./cnvColors');

const [MAP_X, MAP_Y, MAP_W, MAP_H] = usaMap.viewBox.split(/\s+/).map(Number);
const EXTERNAL_MARGIN = 195;
const MAP_VIEWBOX_EXPANDED = `${MAP_X} ${MAP_Y} ${MAP_W + EXTERNAL_MARGIN} ${MAP_H}`;
const EXTERNAL_TEXT_X = MAP_X + MAP_W + EXTERNAL_MARGIN - 16;

/** Callout fuera del mapa (NE + estados pequeños). */
const EXTERNAL_STATE_IDS = [
  'me', 'vt', 'nh', 'ma', 'ri', 'ct', 'nj', 'de', 'md', 'dc', 'wv',
];

const CENTROIDS = buildLabelPointsFromLocations(usaMap.locations);

/**
 * Posición de etiqueta por estado (centro legible dentro del estado).
 * Curadas a mano + validadas sin solapamiento entre cajas de texto.
 */
const LABEL_ANCHORS = {
  ak: [370, 678],
  hi: [603, 621],
  wa: [362, 50],
  or: [362, 122],
  ca: [328, 292],
  nv: [402, 262],
  id: [452, 128],
  mt: [548, 102],
  wy: [562, 188],
  ut: [478, 258],
  co: [598, 278],
  az: [442, 368],
  nm: [558, 392],
  tx: [678, 468],
  nd: [692, 112],
  sd: [692, 172],
  ne: [702, 245],
  ks: [712, 298],
  ok: [705, 372],
  mn: [785, 135],
  wi: [852, 152],
  mi: [815, 96],
  ia: [785, 228],
  il: [842, 268],
  in: [905, 295],
  oh: [958, 252],
  mo: [805, 292],
  ky: [835, 325],
  tn: [900, 348],
  ar: [828, 378],
  ms: [852, 418],
  la: [822, 450],
  al: [912, 415],
  ga: [978, 415],
  sc: [1018, 372],
  nc: [1040, 328],
  va: [1025, 278],
  pa: [1065, 218],
  ny: [1048, 185],
  fl: [955, 478],
};

const INTERNAL_FONT_OVERRIDES = {
  mi: 10,
  pa: 10,
  ak: 10,
  hi: 10,
  nc: 11,
  sc: 11,
  il: 11,
  in: 11,
  ky: 11,
  tn: 11,
  wi: 11,
  oh: 11,
  id: 11,
  fl: 11,
  ny: 11,
  va: 11,
  al: 11,
  ms: 11,
  la: 11,
  ar: 11,
  mo: 11,
  ne: 11,
  ks: 11,
  ok: 11,
  mt: 11,
  wy: 11,
  nd: 11,
  sd: 11,
  ca: 12,
  tx: 12,
};

const SHORT_STATE_NAMES = {
  nc: 'N. Carolina',
  sc: 'S. Carolina',
};

function internalPoint(stateId) {
  if (LABEL_ANCHORS[stateId]) return LABEL_ANCHORS[stateId];
  return CENTROIDS[stateId] || null;
}

const INTERNAL_LABEL_POINTS = Object.fromEntries(
  Object.keys(CENTROIDS)
    .filter(id => !EXTERNAL_STATE_IDS.includes(id))
    .map(id => [id, internalPoint(id)])
    .filter(([, pt]) => pt)
);

const EXTERNAL_FONT_SIZE = 11;

function buildExternalLabels() {
  const entries = EXTERNAL_STATE_IDS.filter(id => CENTROIDS[id]).map(id => {
    const [ax, ay] = CENTROIDS[id];
    return { id, ax, ay, sortY: ay, star: id === 'dc' };
  });
  entries.sort((a, b) => a.sortY - b.sortY);

  const minGap = 18;
  let lastY = null;
  for (const entry of entries) {
    let ty = entry.sortY;
    if (lastY !== null && ty - lastY < minGap) ty = lastY + minGap;
    entry.ty = ty;
    lastY = ty;
  }
  return entries;
}

const EXTERNAL_LABELS = buildExternalLabels();

function internalFontSize(stateId, name) {
  if (INTERNAL_FONT_OVERRIDES[stateId]) return INTERNAL_FONT_OVERRIDES[stateId];
  const n = name.length;
  if (n > 11) return 12;
  if (n > 9) return 13;
  return 14;
}

function displayStateName(loc) {
  if (SHORT_STATE_NAMES[loc.id]) return SHORT_STATE_NAMES[loc.id];
  if (loc.id === 'dc') return 'Washington, DC';
  return loc.name;
}

function stateLabelColor(cnvByAbbr, stateId) {
  const cnv = cnvByAbbr?.has(stateId) ? cnvByAbbr.get(stateId) : 0;
  return cnvColorMeta(cnv).textColor;
}

function externalLabelLayout(name) {
  const fs = EXTERNAL_FONT_SIZE;
  const padX = 6;
  const padY = 3.5;
  const w = Math.ceil(name.length * fs * 0.54 + padX * 2);
  const h = fs + padY * 2;
  return { fs, padX, padY, w, h };
}

function buildExternalLabelSvg(name, x, y, htmlEscape) {
  const { fs, padX, padY, w, h } = externalLabelLayout(name);
  const rx = x - w;
  const ry = y - h / 2;

  return {
    pillLeft: rx,
    svg: `<g class="cov-state-label-callout">
    <rect x="${rx}" y="${ry}" width="${w}" height="${h}" rx="3" class="cov-state-label-pill" />
    <text x="${x - padX}" y="${y}" font-size="${fs}" text-anchor="end" dominant-baseline="middle" class="cov-state-label-callout-text">${htmlEscape(name)}</text>
  </g>`,
  };
}

function buildStateNameLabelsSvg(htmlEscape, locations, cnvByAbbr = null) {
  const byId = Object.fromEntries(locations.map(l => [l.id, l]));
  const parts = [];
  const externalIds = new Set(EXTERNAL_STATE_IDS);

  for (const ext of EXTERNAL_LABELS) {
    const loc = byId[ext.id];
    if (!loc) continue;
    const name = displayStateName(loc);
    const callout = buildExternalLabelSvg(name, EXTERNAL_TEXT_X, ext.ty, htmlEscape);
    parts.push(
      `<line class="cov-label-line" x1="${ext.ax}" y1="${ext.ay}" x2="${callout.pillLeft - 5}" y2="${ext.ty}" />`
    );
    if (ext.star) {
      parts.push(
        `<circle class="cov-dc-marker" cx="${ext.ax}" cy="${ext.ay}" r="4" fill="#2dd4bf" stroke="#0f1419" stroke-width="0.6" />`
      );
    }
    parts.push(callout.svg);
  }

  for (const loc of locations) {
    if (externalIds.has(loc.id)) continue;
    const pt = INTERNAL_LABEL_POINTS[loc.id];
    if (!pt) continue;
    const name = displayStateName(loc);
    const size = internalFontSize(loc.id, name);
    const fill = stateLabelColor(cnvByAbbr, loc.id);
    parts.push(
      `<text class="cov-state-label-internal" x="${pt[0]}" y="${pt[1]}" font-size="${size}" text-anchor="middle" dominant-baseline="middle" fill="${fill}">${htmlEscape(name)}</text>`
    );
  }

  return parts.join('\n');
}

module.exports = {
  buildStateNameLabelsSvg,
  INTERNAL_LABEL_POINTS,
  EXTERNAL_LABELS,
  MAP_VIEWBOX_EXPANDED,
  LABEL_ANCHORS,
};
