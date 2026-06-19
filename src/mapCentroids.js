/** Centro aproximado de un path SVG (M/L/H/V/C/Q) para posicionar etiquetas. */
function pathCentroid(d) {
  const tokens = d.match(/[a-zA-Z]|-?[0-9]*\.?[0-9]+(?:e[-+]?\d+)?/g) || [];
  let i = 0;
  let cmd = '';
  let sx = 0;
  let sy = 0;
  const pts = [];
  const read = () => parseFloat(tokens[i++]);

  while (i < tokens.length) {
    const t = tokens[i];
    if (/[a-zA-Z]/.test(t)) cmd = tokens[i++];
    else if (!cmd) break;

    const rel = cmd === cmd.toLowerCase();
    const up = cmd.toUpperCase();
    let x;
    let y;

    switch (up) {
      case 'M':
        x = read();
        y = read();
        if (rel) {
          x += sx;
          y += sy;
        }
        sx = x;
        sy = y;
        pts.push([x, y]);
        cmd = rel ? 'l' : 'L';
        break;
      case 'L':
        x = read();
        y = read();
        if (rel) {
          x += sx;
          y += sy;
        }
        sx = x;
        sy = y;
        pts.push([x, y]);
        break;
      case 'H':
        x = read();
        if (rel) x += sx;
        sx = x;
        pts.push([sx, sy]);
        break;
      case 'V':
        y = read();
        if (rel) y += sy;
        sy = y;
        pts.push([sx, sy]);
        break;
      case 'C':
        read();
        read();
        read();
        read();
        x = read();
        y = read();
        if (rel) {
          x += sx;
          y += sy;
        }
        sx = x;
        sy = y;
        pts.push([x, y]);
        break;
      case 'Q':
        read();
        read();
        x = read();
        y = read();
        if (rel) {
          x += sx;
          y += sy;
        }
        sx = x;
        sy = y;
        pts.push([x, y]);
        break;
      case 'Z':
        sx = pts[0][0];
        sy = pts[0][1];
        break;
      default:
        i++;
        break;
    }
  }

  if (!pts.length) return [0, 0];
  let cx = 0;
  let cy = 0;
  for (const p of pts) {
    cx += p[0];
    cy += p[1];
  }
  return [Math.round(cx / pts.length), Math.round(cy / pts.length)];
}

function buildLabelPointsFromLocations(locations) {
  const points = {};
  for (const loc of locations || []) {
    points[loc.id] = pathCentroid(loc.path);
  }
  return points;
}

module.exports = { pathCentroid, buildLabelPointsFromLocations };
