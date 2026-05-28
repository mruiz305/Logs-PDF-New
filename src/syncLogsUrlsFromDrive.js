/**
 * CLI: sincroniza URLs sin generar PDFs (solo Drive → MySQL → Glide).
 * Mismos flags que Update URL Logs.
 */
require('dotenv').config();
const { findFileLinkByName } = require('./drive');
const {
  getProdMysqlConnection,
  getMysqlConnection,
  syncLogsUrlForSubmitter,
  buildPdfFileName,
  gUsersTableName,
} = require('./logsUrlSync');
const { generateForOne, puppeteerLaunchOptions, todayYMD } = require('./generateLogsPdf');

function hasFlag(name) {
  return process.argv.includes(`--${name}`);
}

function argValue(name, def = '') {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 && process.argv[i + 1] ? process.argv[i + 1].trim() : def;
}

function getSyncDateRange() {
  const from = (process.env.SYNC_LOGS_FROM || '2024-01-01').trim();
  const to = (process.env.SYNC_LOGS_TO || todayYMD()).trim();
  return { from, to };
}

async function fetchUsersToSync(
  conn,
  allMarketing = false,
  singleEmail = '',
  hrStatusFilter = 'TERMED',
  onlyMissingUrl = false
) {
  const gu = gUsersTableName();
  let sql = `
    SELECT email, name, logsIndividualFile, hrStatus
    FROM ${gu}
    WHERE systemDepartment = 'Marketing'
      AND TRIM(COALESCE(NULLIF(name,''), email)) <> ''
  `;
  const params = [];
  if (allMarketing) {
    // sin filtro hrStatus
  } else if (hrStatusFilter) {
    sql += ` AND UPPER(TRIM(COALESCE(hrStatus,''))) = ?`;
    params.push(hrStatusFilter);
  }
  if (singleEmail) {
    sql += ` AND LOWER(TRIM(email)) = LOWER(?)`;
    params.push(singleEmail);
  }
  if (onlyMissingUrl) {
    sql += ` AND (logsIndividualFile IS NULL OR TRIM(COALESCE(logsIndividualFile,'')) = '')`;
  }
  sql += ` ORDER BY email`;
  const [rows] = await conn.execute(sql, params);
  return rows;
}

async function run() {
  const allMarketing = hasFlag('all-marketing');
  const activeNoUrl = hasFlag('active-no-url');
  const dryRun = hasFlag('dry-run');
  const noGenerate = hasFlag('no-generate');
  const singleEmail = argValue('email', '');
  const { from: rangeFrom, to: rangeTo } = getSyncDateRange();

  if (dryRun) console.log('Modo dry-run: no se realizarán cambios.\n');

  const prodConn = await getProdMysqlConnection();
  let stgConn = null;
  try {
    stgConn = await getMysqlConnection();
  } catch (e) {
    console.warn('↪ Sin conexión performance (stg):', e.message || e);
  }

  const hrStatusFilter = allMarketing ? '' : activeNoUrl ? 'ACTIVE' : 'TERMED';
  const onlyMissingUrl = activeNoUrl && !allMarketing;
  const users = await fetchUsersToSync(
    prodConn,
    allMarketing,
    singleEmail,
    hrStatusFilter,
    onlyMissingUrl
  );

  console.log(`Usuarios a procesar: ${users.length}\n`);
  console.log(`Rango PDF si hace falta generar: ${rangeFrom} .. ${rangeTo}\n`);

  let updated = 0;
  let unchanged = 0;
  let errors = 0;
  let generated = 0;
  let missingStill = 0;

  let browser = null;
  async function ensureBrowser() {
    if (!browser) browser = await puppeteer.launch(puppeteerLaunchOptions());
    return browser;
  }

  for (const user of users) {
    const submitter =
      user.name && String(user.name).trim()
        ? String(user.name).trim()
        : String(user.email || '').trim();
    const fileName = buildPdfFileName(submitter);

    try {
      let found = await findFileLinkByName(fileName);
      let driveLink = found ? found.webViewLink.trim() : null;

      if (!driveLink) {
        if (dryRun) {
          console.log(`[DRY-RUN] ${user.email}: sin "${fileName}" en Drive`);
          missingStill++;
          continue;
        }
        if (noGenerate) {
          console.log(`⚠ Sin PDF en Drive, --no-generate: ${user.email}`);
          missingStill++;
          continue;
        }
        if (!stgConn) {
          stgConn = await getMysqlConnection();
        }
        console.log(`⏳ Generando PDF… ${user.email}`);
        const br = await ensureBrowser();
        const result = await generateForOne(stgConn, br, submitter, rangeFrom, rangeTo, {
          forceUpload: true,
          skipUrlSync: true,
        });
        if (!result.driveLink) {
          console.error(`✗ Sin URL Drive tras generar: ${user.email}`);
          errors++;
          continue;
        }
        driveLink = result.driveLink.trim();
        generated++;
      }

      const status = await syncLogsUrlForSubmitter(stgConn, submitter, {
        driveLink,
        dryRun,
      });
      if (status === 'updated') updated++;
      else if (status === 'unchanged') unchanged++;
      else if (status === 'missing') missingStill++;
      else if (status === 'error') errors++;
    } catch (e) {
      errors++;
      console.error(`✗ ${user.email}:`, e.message || e);
    }
  }

  if (browser) await browser.close();
  await prodConn.end();
  if (stgConn) await stgConn.end();

  console.log('\n--- Resumen sync-urls ---');
  console.log(`PDFs generados: ${generated}`);
  console.log(`URLs actualizadas: ${updated}`);
  console.log(`Sin cambios: ${unchanged}`);
  console.log(`Sin PDF / pendientes: ${missingStill}`);
  console.log(`Errores: ${errors}`);
}

run().catch(e => {
  console.error(e);
  process.exit(1);
});
