/**
 * Sincroniza logsIndividualFile: Google Drive → MySQL producción (g_users) → Glide.
 * Las consultas del PDF siguen en DB_* (performance_data); URLs en PROD_DB_* (dbProduction).
 */
require('dotenv').config();
const mysql = require('mysql2/promise');
const { findFileLinkByName } = require('./drive');
const {
  syncLogsUrlToGlide,
  isGlideConfigured,
  formatGlideSyncResult,
} = require('./glideClient');

function gUsersTableName() {
  const t = (process.env.G_USERS_TABLE || 'g_users').trim();
  if (!/^[a-zA-Z0-9_.]+$/.test(t)) {
    throw new Error('G_USERS_TABLE inválido (solo letras, números, _ y .)');
  }
  return t;
}

function stgUsersTableName() {
  const t = (process.env.STG_G_USERS_TABLE || 'stg_g_users').trim();
  if (!/^[a-zA-Z0-9_.]+$/.test(t)) {
    throw new Error('STG_G_USERS_TABLE inválido');
  }
  return t;
}

function buildPdfFileName(submitter) {
  const base = `Logs_ ${(submitter || '').trim()}`.replace(/[\\/:*?"<>|]+/g, ' ').trim();
  return `${base}.pdf`;
}

function urlsMatch(a, b) {
  const na = (a || '').trim();
  const nb = (b || '').trim();
  if (!na && !nb) return true;
  if (!na || !nb) return false;
  return na === nb;
}

function syncEnabled() {
  return (process.env.SYNC_URLS_AFTER_PDF || 'true').toLowerCase() !== 'false';
}

/** Conexión performance_data (reportes / stg_g_users) */
async function getMysqlConnection() {
  const cfg = {
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_DATABASE,
    charset: 'utf8mb4',
    dateStrings: true,
  };
  if (process.env.DB_PORT) {
    cfg.port = parseInt(process.env.DB_PORT, 10);
  }
  return mysql.createConnection(cfg);
}

/** Conexión producción (g_users / Glide source of truth) */
async function getProdMysqlConnection() {
  const host = process.env.PROD_DB_HOST;
  if (!host) {
    throw new Error(
      'Faltan PROD_DB_HOST (y PROD_DB_USER, PROD_DB_PASSWORD, PROD_DB_DATABASE) en .env para sincronizar URLs.'
    );
  }
  const cfg = {
    host,
    user: process.env.PROD_DB_USER,
    password: process.env.PROD_DB_PASSWORD,
    database: process.env.PROD_DB_DATABASE || 'dbProduction',
    charset: 'utf8mb4',
    dateStrings: true,
  };
  if (process.env.PROD_DB_PORT) {
    cfg.port = parseInt(process.env.PROD_DB_PORT, 10);
  }
  return mysql.createConnection(cfg);
}

async function fetchUserForSync(prodConn, submitterName) {
  const gu = gUsersTableName();
  const [rows] = await prodConn.execute(
    `
      SELECT email, name, logsIndividualFile
      FROM ${gu}
      WHERE TRIM(name) = TRIM(?)
         OR LOWER(TRIM(email)) = LOWER(TRIM(?))
      LIMIT 1
    `,
    [submitterName, submitterName]
  );
  return rows[0] || null;
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function updateLogsUrlInMysql(prodConn, email, url) {
  const gu = gUsersTableName();
  const sql = `UPDATE ${gu} SET logsIndividualFile = ? WHERE LOWER(TRIM(email)) = LOWER(TRIM(?))`;
  const params = [url, email];
  const maxAttempts = parseInt(process.env.SYNC_URL_UPDATE_RETRIES || '2', 10);

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const [result] = await prodConn.execute(sql, params);
      return result.affectedRows > 0;
    } catch (e) {
      const lockTimeout = e.code === 'ER_LOCK_WAIT_TIMEOUT';
      if (lockTimeout && attempt < maxAttempts) {
        const waitMs = parseInt(process.env.SYNC_URL_RETRY_MS || '3000', 10);
        console.warn(
          `  ↪ Lock en ${gu}, reintento ${attempt + 1}/${maxAttempts} en ${waitMs}ms…`
        );
        await sleep(waitMs);
        continue;
      }
      if (lockTimeout) {
        console.warn(
          `  ⚠ Lock wait timeout al actualizar ${gu} (${email}). Otro proceso puede estar usando la fila.`
        );
        return false;
      }
      throw e;
    }
  }
  return false;
}

/** Actualiza stg_g_users en performance_data (conexión distinta a producción) */
async function updateStgLogsUrlIfNeeded(stgConn, submitterName, url) {
  if (!stgConn) return false;
  const stg = stgUsersTableName();
  const [result] = await stgConn.execute(
    `UPDATE ${stg} SET logsIndividualFile = ?
     WHERE TRIM(name) = TRIM(?) OR LOWER(TRIM(email)) = LOWER(TRIM(?))`,
    [url, submitterName, submitterName]
  );
  return result.affectedRows > 0;
}

/**
 * @param {import('mysql2/promise').Connection|null} stgConn - DB performance (stg_g_users); puede ser null
 */
async function syncLogsUrlForSubmitter(stgConn, submitterName, options = {}) {
  const { driveLink: driveLinkHint = null, dryRun = false } = options;

  if (!submitterName || !String(submitterName).trim()) {
    return 'skipped';
  }

  const prodDb = process.env.PROD_DB_DATABASE || 'dbProduction';
  let prodConn;
  try {
    prodConn = await getProdMysqlConnection();

    const user = await fetchUserForSync(prodConn, submitterName.trim());
    if (!user) {
      console.log(
        `  ↪ URL sync: sin fila en ${prodDb}.${gUsersTableName()} para "${submitterName}"`
      );
      return 'skipped';
    }

    const fileName = buildPdfFileName(
      (user.name && String(user.name).trim()) || submitterName
    );
    const currentUrl = (user.logsIndividualFile || '').trim();
    let correctUrl = (driveLinkHint || '').trim() || null;

    if (!correctUrl) {
      const found = await findFileLinkByName(fileName);
      if (found) correctUrl = found.webViewLink.trim();
    }

    if (!correctUrl) {
      console.log(`  ⚠ URL sync: no hay PDF en Drive (${fileName}) para ${user.email}`);
      return 'missing';
    }

    if (urlsMatch(currentUrl, correctUrl)) {
      return 'unchanged';
    }

    if (dryRun) {
      console.log(`  [DRY-RUN] ${user.email}: ${currentUrl || '(vacío)'} → ${correctUrl}`);
      return 'updated';
    }

    const mysqlOk = await updateLogsUrlInMysql(prodConn, user.email, correctUrl);
    const stgOk = await updateStgLogsUrlIfNeeded(
      stgConn,
      (user.name && String(user.name).trim()) || submitterName,
      correctUrl
    );

    let glideResult = null;
    let glideSkipped = false;
    if ((process.env.SYNC_GLIDE || 'true').toLowerCase() === 'false') {
      glideSkipped = true;
    } else if (!isGlideConfigured()) {
      console.log(`  ↪ Glide omitido (faltan variables GLIDE_* en .env)`);
      glideSkipped = true;
    } else {
      try {
        glideResult = await syncLogsUrlToGlide(user.email, correctUrl, {
          main: true,
          profile: true,
          onlyIfEmpty: false,
          onlyIfDifferent: true,
        });
      } catch (e) {
        console.warn(`  ⚠ Glide (${user.email}):`, e.message || e);
      }
    }

    if (mysqlOk) {
      const parts = [`${prodDb}.${gUsersTableName()}`];
      if (stgOk) parts.push(`${process.env.DB_DATABASE || 'staging'}.${stgUsersTableName()}`);
      if (glideResult) parts.push(formatGlideSyncResult(glideResult));
      else if (!glideSkipped) parts.push('Glide (error)');
      console.log(`  ✔ URL sync ${user.email}: ${parts.join(' + ')}`);
    } else {
      console.warn(`  ⚠ URL sync: no se pudo actualizar producción para ${user.email}`);
    }

    return mysqlOk ? 'updated' : 'error';
  } catch (e) {
    if (e.code === 'ER_LOCK_WAIT_TIMEOUT') {
      console.warn(`  ⚠ URL sync: lock timeout en producción (${submitterName.trim()})`);
      return 'error';
    }
    throw e;
  } finally {
    if (prodConn) await prodConn.end();
  }
}

module.exports = {
  syncEnabled,
  syncLogsUrlForSubmitter,
  buildPdfFileName,
  urlsMatch,
  gUsersTableName,
  getMysqlConnection,
  getProdMysqlConnection,
  fetchUserForSync,
};
