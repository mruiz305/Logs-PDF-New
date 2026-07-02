/**
 * Glide: logsIndividualFile existe en dos tablas.
 * - Main (GLIDE_TABLE_ID): columna nwYAC (uri)
 * - Profile (GLIDE_PROFILE_TABLE_ID): columna SKVBe (string)
 */
const MAIN_COLUMNS = {
  email: { type: 'email-address', name: 'e0eY9' },
  logsIndividualFile: { type: 'uri', name: 'nwYAC' },
};

const PROFILE_COLUMNS = {
  email: { type: 'email-address', name: 'Ijob6' },
  logsIndividualFile: { type: 'string', name: 'SKVBe' },
};

let glideCache = null;

function glideCredentials() {
  const token = process.env.GLIDE_TABLE_TOKEN || process.env.GLIDE_TOKEN;
  const app = process.env.GLIDE_APP_ID;
  if (!token || !app) {
    throw new Error(
      'Faltan GLIDE_TABLE_TOKEN (o GLIDE_TOKEN) o GLIDE_APP_ID en .env'
    );
  }
  return { token, app };
}

function buildGlideTable(tableId, columns) {
  const glide = require('@glideapps/tables');
  const { token, app } = glideCredentials();
  if (!tableId) {
    throw new Error('Falta tableId de Glide');
  }
  return glide.table({ token, app, table: tableId, columns });
}

function getGlideMainTable() {
  const tableId = process.env.GLIDE_TABLE_ID;
  if (!tableId) {
    throw new Error('Falta GLIDE_TABLE_ID en .env');
  }
  return buildGlideTable(tableId, MAIN_COLUMNS);
}

function getGlideProfileTable() {
  const tableId = process.env.GLIDE_PROFILE_TABLE_ID;
  if (!tableId) {
    throw new Error('Falta GLIDE_PROFILE_TABLE_ID en .env');
  }
  return buildGlideTable(tableId, PROFILE_COLUMNS);
}

/** @deprecated use getGlideMainTable */
function getGlideTable() {
  return getGlideMainTable();
}

function isGlideConfigured() {
  return !!(
    (process.env.GLIDE_TABLE_TOKEN || process.env.GLIDE_TOKEN) &&
    process.env.GLIDE_APP_ID &&
    process.env.GLIDE_TABLE_ID
  );
}

function isGlideProfileConfigured() {
  return isGlideConfigured() && !!process.env.GLIDE_PROFILE_TABLE_ID;
}

function rowsByEmail(rows) {
  return new Map(
    (rows || [])
      .filter(r => String(r.email || '').trim())
      .map(r => [String(r.email || '').trim().toLowerCase(), r])
  );
}

async function loadGlideCache(forceRefresh = false) {
  if (glideCache && !forceRefresh) return glideCache;

  const mainTable = getGlideMainTable();
  const mainRows = await mainTable.get();
  const cache = {
    mainTable,
    mainByEmail: rowsByEmail(mainRows),
    profileTable: null,
    profileByEmail: new Map(),
  };

  if (isGlideProfileConfigured()) {
    cache.profileTable = getGlideProfileTable();
    const profileRows = await cache.profileTable.get();
    cache.profileByEmail = rowsByEmail(profileRows);
  }

  glideCache = cache;
  return glideCache;
}

function clearGlideCache() {
  glideCache = null;
}

/**
 * @param {object} options
 * @param {boolean} [options.main=true]
 * @param {boolean} [options.profile=true]
 * @param {boolean} [options.onlyIfEmpty=false] solo escribe si la celda está vacía
 * @param {boolean} [options.onlyIfDifferent=true] no reescribe si ya tiene la misma URL
 * @returns {Promise<{mainUpdated:boolean,profileUpdated:boolean,mainFound:boolean,profileFound:boolean}>}
 */
async function syncLogsUrlToGlide(email, logsUrl, options = {}) {
  const {
    main = true,
    profile = true,
    onlyIfEmpty = false,
    onlyIfDifferent = true,
    cache = null,
  } = options;

  const url = String(logsUrl || '').trim();
  if (!url) {
    return {
      mainUpdated: false,
      profileUpdated: false,
      mainFound: false,
      profileFound: false,
    };
  }

  const lookup = cache || (await loadGlideCache());
  const key = String(email || '').trim().toLowerCase();
  const result = {
    mainUpdated: false,
    profileUpdated: false,
    mainFound: false,
    profileFound: false,
  };

  async function maybeUpdate(table, row, field) {
    const current = String(row[field] || '').trim();
    if (onlyIfEmpty && current) return false;
    if (onlyIfDifferent && current === url) return false;
    await table.update(row.$rowID, { [field]: url });
    row[field] = url;
    return true;
  }

  if (main) {
    const row = lookup.mainByEmail.get(key);
    if (row) {
      result.mainFound = true;
      result.mainUpdated = await maybeUpdate(
        lookup.mainTable,
        row,
        'logsIndividualFile'
      );
    }
  }

  if (profile && lookup.profileTable) {
    const row = lookup.profileByEmail.get(key);
    if (row) {
      result.profileFound = true;
      result.profileUpdated = await maybeUpdate(
        lookup.profileTable,
        row,
        'logsIndividualFile'
      );
    }
  }

  return result;
}

/**
 * Actualiza main + profile en el flujo normal (siempre que haya URL nueva).
 * @returns {Promise<boolean>} true si al menos una tabla se actualizó o ya tenía la URL
 */
async function updateLogsUrlInGlide(email, logsUrl) {
  const result = await syncLogsUrlToGlide(email, logsUrl, {
    main: true,
    profile: true,
    onlyIfEmpty: false,
    onlyIfDifferent: true,
  });
  return result.mainUpdated || result.profileUpdated || result.mainFound || result.profileFound;
}

function formatGlideSyncResult(result, { skipped = false } = {}) {
  if (skipped) return 'Glide (omitido)';
  const parts = [];
  if (result.mainUpdated) parts.push('Glide-main');
  else if (result.mainFound) parts.push('Glide-main (sin cambio)');
  else parts.push('Glide-main (no encontrado)');

  if (isGlideProfileConfigured()) {
    if (result.profileUpdated) parts.push('Glide-profile');
    else if (result.profileFound) parts.push('Glide-profile (sin cambio)');
    else parts.push('Glide-profile (no encontrado)');
  }
  return parts.join(' + ');
}

module.exports = {
  getGlideTable,
  getGlideMainTable,
  getGlideProfileTable,
  isGlideConfigured,
  isGlideProfileConfigured,
  loadGlideCache,
  clearGlideCache,
  syncLogsUrlToGlide,
  updateLogsUrlInGlide,
  formatGlideSyncResult,
};
