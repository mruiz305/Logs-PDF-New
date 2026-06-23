/**
 * CLI: sincroniza logsIndividualFile desde g_users (producción) hacia Glide.
 * Útil cuando MySQL ya tiene URLs actualizadas pero Glide quedó atrasado.
 */
require('./loadEnv');
const {
  getProdMysqlConnection,
  gUsersTableName,
} = require('./logsUrlSync');
const {
  getGlideTable,
  isGlideConfigured,
} = require('./glideClient');

function hasFlag(name) {
  return process.argv.includes(`--${name}`);
}

function argValue(name, def = '') {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 && process.argv[i + 1] ? process.argv[i + 1].trim() : def;
}

async function fetchUsersWithLogsUrl(conn, { all = false, email = '' } = {}) {
  const gu = gUsersTableName();
  let sql = `
    SELECT email, name, logsIndividualFile
    FROM ${gu}
    WHERE logsIndividualFile IS NOT NULL
      AND TRIM(logsIndividualFile) <> ''
      AND TRIM(COALESCE(email, '')) <> ''
  `;
  const params = [];

  if (!all) {
    sql += ` AND systemDepartment = 'Marketing'`;
  }

  if (email) {
    sql += ` AND LOWER(TRIM(email)) = LOWER(?)`;
    params.push(email);
  }

  sql += ` ORDER BY email`;
  const [rows] = await conn.execute(sql, params);
  return rows;
}

async function run() {
  const dryRun = hasFlag('dry-run');
  const all = hasFlag('all');
  const overwrite = hasFlag('overwrite');
  const email = argValue('email', '');

  if (!isGlideConfigured()) {
    throw new Error('Faltan variables GLIDE_TABLE_TOKEN/GLIDE_TOKEN, GLIDE_APP_ID o GLIDE_TABLE_ID.');
  }

  const conn = await getProdMysqlConnection();
  const glideTable = getGlideTable();
  let updated = 0;
  let skippedHasUrl = 0;
  let missingInGlide = 0;
  let errors = 0;

  try {
    const users = await fetchUsersWithLogsUrl(conn, { all, email });
    const glideRows = await glideTable.get();
    const glideByEmail = new Map(
      glideRows
        .filter(r => String(r.email || '').trim())
        .map(r => [String(r.email || '').trim().toLowerCase(), r])
    );

    console.log(`Usuarios con logsIndividualFile en g_users: ${users.length}`);
    console.log(
      overwrite
        ? 'Modo overwrite: actualiza Glide aunque ya tenga URL.'
        : 'Modo default: solo actualiza Glide si logsIndividualFile está vacío.'
    );
    if (dryRun) console.log('Modo dry-run: no se actualiza Glide.\n');

    for (const user of users) {
      const userEmail = String(user.email || '').trim();
      const url = String(user.logsIndividualFile || '').trim();
      try {
        const glideRow = glideByEmail.get(userEmail.toLowerCase());
        if (!glideRow) {
          missingInGlide++;
          console.warn(`⚠ Email no encontrado en Glide: ${userEmail}`);
          continue;
        }

        const currentGlideUrl = String(glideRow.logsIndividualFile || '').trim();
        if (currentGlideUrl && !overwrite) {
          skippedHasUrl++;
          continue;
        }

        if (dryRun) {
          console.log(
            `[DRY-RUN] ${userEmail}: ${currentGlideUrl || '(vacío)'} → ${url}`
          );
          updated++;
          continue;
        }

        await glideTable.update(glideRow.$rowID, { logsIndividualFile: url });
        updated++;
        console.log(`✔ Glide actualizado: ${userEmail}`);
      } catch (e) {
        errors++;
        console.error(`✗ ${userEmail}:`, e.message || e);
      }
    }
  } finally {
    await conn.end();
  }

  console.log('\n--- Resumen g_users → Glide ---');
  console.log(`Actualizados / candidatos dry-run: ${updated}`);
  console.log(`Omitidos porque Glide ya tenía URL: ${skippedHasUrl}`);
  console.log(`No encontrados en Glide: ${missingInGlide}`);
  console.log(`Errores: ${errors}`);
}

run().catch(e => {
  console.error(e.message || e);
  process.exit(1);
});
