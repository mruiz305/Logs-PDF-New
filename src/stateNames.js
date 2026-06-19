/** Nombre completo / abreviatura → id del mapa (@svg-maps/usa usa 2 letras minúsculas). */
const STATE_NAME_TO_ABBR = {
  alabama: 'al',
  alaska: 'ak',
  arizona: 'az',
  arkansas: 'ar',
  california: 'ca',
  colorado: 'co',
  connecticut: 'ct',
  delaware: 'de',
  'district of columbia': 'dc',
  florida: 'fl',
  georgia: 'ga',
  hawaii: 'hi',
  idaho: 'id',
  illinois: 'il',
  indiana: 'in',
  iowa: 'ia',
  kansas: 'ks',
  kentucky: 'ky',
  louisiana: 'la',
  maine: 'me',
  maryland: 'md',
  massachusetts: 'ma',
  michigan: 'mi',
  minnesota: 'mn',
  mississippi: 'ms',
  missouri: 'mo',
  montana: 'mt',
  nebraska: 'ne',
  nevada: 'nv',
  'new hampshire': 'nh',
  'new jersey': 'nj',
  'new mexico': 'nm',
  'new york': 'ny',
  'north carolina': 'nc',
  'north dakota': 'nd',
  ohio: 'oh',
  oklahoma: 'ok',
  oregon: 'or',
  pennsylvania: 'pa',
  'rhode island': 'ri',
  'south carolina': 'sc',
  'south dakota': 'sd',
  tennessee: 'tn',
  texas: 'tx',
  utah: 'ut',
  vermont: 'vt',
  virginia: 'va',
  washington: 'wa',
  'west virginia': 'wv',
  wisconsin: 'wi',
  wyoming: 'wy',
  'washington dc': 'dc',
  'washington d.c.': 'dc',
  'washington, dc': 'dc',
};

/** Región → abreviaturas (disjuntas, cubren los 50 estados + DC). */
const COVERAGE_REGIONS = [
  { label: 'WEST', abbrs: ['ak', 'hi', 'id', 'mt', 'nv', 'wy', 'wa', 'or', 'ca', 'co', 'ut'] },
  { label: 'MIDWEST', abbrs: ['ia', 'mn', 'mo', 'ne', 'nd', 'sd', 'il', 'in', 'ks', 'mi', 'oh', 'wi'] },
  { label: 'SOUTHWEST', abbrs: ['az', 'nm', 'ok', 'tx'] },
  { label: 'SOUTH', abbrs: ['la', 'ar', 'ms', 'al', 'tn', 'ky', 'wv', 'va', 'nc', 'sc', 'ga', 'fl', 'md', 'de'] },
  { label: 'NORTHEAST', abbrs: ['vt', 'nh', 'ri', 'ct', 'me', 'ma', 'ny', 'nj', 'pa', 'dc'] },
];

function normalizeStateAbbr(state) {
  const raw = String(state ?? '').trim();
  if (!raw) return null;
  if (/^[a-zA-Z]{2}$/.test(raw)) return raw.toLowerCase();
  const key = raw.toLowerCase().replace(/\./g, '');
  return STATE_NAME_TO_ABBR[key] || null;
}

function buildCnvLookup(statesRows) {
  const map = new Map();
  for (const row of statesRows || []) {
    const abbr = normalizeStateAbbr(row.state);
    if (abbr) map.set(abbr, row.cnv);
  }
  return map;
}

module.exports = {
  STATE_NAME_TO_ABBR,
  COVERAGE_REGIONS,
  normalizeStateAbbr,
  buildCnvLookup,
};
