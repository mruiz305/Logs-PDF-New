const fs = require('fs');
const path = require('path');

function loadUsaMap() {
  const pkgPath = require.resolve('@svg-maps/usa');
  const source = fs.readFileSync(pkgPath, 'utf8').trim();
  const json = source.replace(/^export\s+default\s+/, '').replace(/;$/, '');
  return JSON.parse(json);
}

module.exports = loadUsaMap();
