const usaMap = require('./usaMap');
const { buildLabelPointsFromLocations } = require('./mapCentroids');
const { cnvColorMeta } = require('./cnvColors');

const [MAP_X, MAP_Y, MAP_W, MAP_H] = usaMap.viewBox.split(/\s+/).map(Number);
const EXTERNAL_MARGIN = 245;
const MAP_VIEWBOX_EXPANDED = `${MAP_X} ${MAP_Y} ${MAP_W + EXTERNAL_MARGIN} ${MAP_H}`;
const EXTERNAL_TEXT_X = MAP_X + MAP_W + EXTERNAL_MARGIN - 16;

/** Callout fuera del mapa (NE + estados pequeños). */
const EXTERNAL_STATE_IDS = [
  'me', 'vt', 'nh', 'ma', 'ri', 'ct', 'nj', 'de', 'md', 'dc',
];

const CENTROIDS = buildLabelPointsFromLocations(usaMap.locations);

/**
 * Posición de etiqueta por estado (centro legible dentro del estado).
 * Curadas a mano + validadas sin solapamiento entre cajas de texto.
 */
const LABEL_ANCHORS = {
  ak: [370, 678],
  hi: [603, 621],
  // --- West ---
  wa: [400,  52],  // slight east of geo-center compensates for Puget Sound indentation
  or: [375, 126],
  ca: [342, 272],  // y=272 cx=342 w=73 symmetric (geo-center 362 skewed north; current narrows in mid-section)
  nv: [411, 258],
  id: [473, 160],  // panhandle ends ≈y=130; widest body at y=160 w=99 cx=475
  mt: [556,  98],
  wy: [576, 191],
  ut: [496, 259],
  co: [599, 284],
  az: [473, 376],
  nm: [578, 385],
  // --- South/Central ---
  tx: [690, 466],
  nd: [699, 102],
  sd: [697, 174],
  ne: [704, 235],
  ks: [724, 303],
  ok: [743, 380],  // main body cx=743 (panhandle ends at y≈360; geo-center 717 was inside panhandle)
  mn: [798, 104],  // y=104 w=90 cx=798 → 11.5px margins; y=112 w=83 was still under 10px
  wi: [858, 160],  // y=160 w=88 cx=860 main body; y=174 w=69 was Door Peninsula narrowing
  mi: [952, 182],  // lower peninsula widest: y=180 w=66 cx=952 (old 935 was off-center left)
  ia: [809, 226],
  il: [880, 278],  // y=280 cx=880; prior x=876 was 4 SVG left of center
  in: [933, 263],
  oh: [989, 247],
  mo: [832, 312],  // y=312 w=85 cx=832 → 11.7px symmetric; y=306 w=75 had R=7.2px only
  ky: [948, 316],
  tn: [948, 352],
  ar: [832, 386],  // y=384 cx=832; prior x=835 off-center (L=12.6px vs R=8.5px)
  ms: [881, 456],  // y=456 w=59 cx=881 widest mid-section (symmetric 2.1px; y=440 w=50 < text width)
  la: [848, 468],  // y=464 cx=848; prior x=854 off-center (L=13.2px vs R=4.0px)
  al: [941, 427],
  ga: [1007, 415],  // y=416 cx=1007; prior x=1004 off-center (L=9.9px vs R=14.0px)
  sc: [1043, 376],  // y=376 widest row w=90 cx=1043; y=396 w=61 was narrower than compressed text
  nc: [1058, 342],
  // --- Northeast ---
  va: [1068, 312],  // east+south for VA/WV separation
  wv: [1051, 256],  // y=256 w=56 cx=1051 widest section; clear of OH label (x gap 50+ SVG)
  pa: [1075, 221],  // y=224 cx=1075; prior x=1073 off-center (L=9.4px vs R=11.9px)
  ny: [1090, 170],  // upstate wide section: y=170 w=95 cx=1089 (Long Island pulls geo-center east)
  fl: [1058, 528],  // peninsula y=528 w=56 cx=1058; y=500 label was 16 SVG left of true cx
};

const INTERNAL_FONT_OVERRIDES = {
  mi: 11,
  pa: 11,
  nc: 11,
  sc: 11,
  il: 10,
  in: 10,
  ky: 12,  // bumped for readability between crowded neighbors
  tn: 11,
  wi: 11,
  oh: 11,
  id: 12,
  fl: 11,
  ny: 11,
  va: 10,
  wv: 10,  // bumped from 9; fits 13-char "West Virginia" with textLength
  al: 11,
  ms:  9,  // bumped from 8; fits 11-char "Mississippi" with textLength
  la: 11,
  ar: 11,
  mo: 12,
  ne: 11,
  ks: 11,
  ok: 11,
  mt: 12,
  wy: 12,
  nd: 11,
  sd: 11,
  ca: 11,  // at y=268 state is 87px wide; 12px text overflows right side
  tx: 12,
};

const SHORT_STATE_NAMES = {};

const INTERNAL_MAX_WIDTHS = {
  wa: 100,
  or: 120,
  ca: 120,
  id:  90,
  mt: 152,
  wy: 105,
  nd:  98,
  sd: 104,
  ne: 124,
  ks: 112,
  ok: 130,
  mn:  98,
  wi:  80,
  mi:  68,
  ia:  90,
  il:  60,
  in:  46,
  oh:  64,
  mo: 100,
  ky: 112,
  tn: 128,
  ar:  76,
  ms:  58,  // 11ch × 9px × 0.55 = 54.45; need maxWidth > 54.45
  al:  58,
  la:  88,
  ga:  82,
  fl: 138,
  nc: 132,
  sc:  76,
  va: 120,
  wv:  52,  // state max w=56 at y=256; compress text to 52 for 1.5px CSS margins each side
  pa:  88,
  ny: 114,
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

const EXTERNAL_FONT_SIZE = 12;

function buildExternalLabels() {
  const entries = EXTERNAL_STATE_IDS.filter(id => CENTROIDS[id]).map(id => {
    const [ax, ay] = CENTROIDS[id];
    return { id, ax, ay, sortY: ay, star: id === 'dc' };
  });
  // Geographic north-to-south order — verified via true segment intersection
  // check: these nearly-horizontal lines produce zero actual crossings.
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
  if (n > 11) return 10;
  if (n > 9)  return 11;
  return 12;
}

function fitLabelAttrs(stateId, name, fontSize) {
  const maxWidth = INTERNAL_MAX_WIDTHS[stateId];
  if (!maxWidth) return '';
  const estimated = name.length * fontSize * 0.55;
  if (estimated <= maxWidth) return '';
  return ` textLength="${maxWidth}" lengthAdjust="spacingAndGlyphs"`;
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
  const w = Math.ceil(name.length * fs * 0.55);
  return { fs, w };
}

function buildExternalLabelSvg(name, x, y, htmlEscape) {
  const { fs, w } = externalLabelLayout(name);
  return {
    textLeft: x - w,
    svg: `<text x="${x}" y="${y}" font-size="${fs}" text-anchor="end" dominant-baseline="middle" class="cov-state-label-callout-text">${htmlEscape(name)}</text>`,
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
      `<line class="cov-label-line" x1="${ext.ax}" y1="${ext.ay}" x2="${callout.textLeft - 8}" y2="${ext.ty}" />`
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
    const fitAttrs = fitLabelAttrs(loc.id, name, size);
    parts.push(
      `<text class="cov-state-label-internal" x="${pt[0]}" y="${pt[1]}" font-size="${size}" text-anchor="middle" dominant-baseline="middle" fill="${fill}"${fitAttrs}>${htmlEscape(name)}</text>`
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
