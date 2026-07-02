/**
 * CLI: backfill de logsIndividualFile en la tabla PROFILE de Glide.
 * Por defecto solo llena celdas vacías (desde g_users producción).
 *
 * npm run sync-glide-profile:dry
 * npm run sync-glide-profile
 * npm run sync-glide-profile -- --overwrite
 * npm run sync-glide-profile -- --email user@example.com
 */
require('./loadEnv');
const {
  getProdMysqlConnection,
  gUsersTableName,
} = require('./logsUrlSync');
const {
  isGlideProfileConfigured,
  loadGlideCache,
  clearGlideCache,
  syncLogsUrlToGlide,
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

  if (!isGlideProfileConfigured()) {
    throw new Error(
      'Faltan GLIDE_PROFILE_TABLE_ID (y GLIDE_TABLE_TOKEN, GLIDE_APP_ID) en .env'
    );
  }

  const conn = await getProdMysqlConnection();
  clearGlideCache();
  const cache = await loadGlideCache(true);

  let updated = 0;
  let skippedHasUrl = 0;
  let missingInProfile = 0;
  let errors = 0;

  try {
    const users = await fetchUsersWithLogsUrl(conn, { all, email });

    console.log(`Usuarios con logsIndividualFile en g_users: ${users.length}`);
    console.log('Destino: Glide PROFILE (SKVBe / logsIndividualFile)');
    console.log(
      overwrite
        ? 'Modo overwrite: actualiza aunque profile ya tenga URL.'
        : 'Modo default: solo actualiza profile si logsIndividualFile está vacío.'
    );
    if (dryRun) console.log('Modo dry-run: no se escribe en Glide.\n');

    for (const user of users) {
      const userEmail = String(user.email || '').trim();
      const url = String(user.logsIndividualFile || '').trim();
      try {
        const profileRow = cache.profileByEmail.get(userEmail.toLowerCase());
        if (!profileRow) {
          missingInProfile++;
          console.warn(`⚠ Email no encontrado en Glide profile: ${userEmail}`);
          continue;
        }

        const currentProfileUrl = String(profileRow.logsIndividualFile || '').trim();
        if (currentProfileUrl && !overwrite) {
          skippedHasUrl++;
          continue;
        }
        if (currentProfileUrl === url) {
          skippedHasUrl++;
          continue;
        }

        if (dryRun) {
          console.log(
            `[DRY-RUN] ${userEmail}: ${currentProfileUrl || '(vacío)'} → ${url}`
          );
          updated++;
          continue;
        }

        const result = await syncLogsUrlToGlide(userEmail, url, {
          main: false,
          profile: true,
          onlyIfEmpty: !overwrite,
          onlyIfDifferent: true,
          cache,
        });

        if (result.profileUpdated) {
          updated++;
          console.log(`✔ Glide profile actualizado: ${userEmail}`);
        } else if (result.profileFound) {
          skippedHasUrl++;
        } else {
          missingInProfile++;
        }
      } catch (e) {
        errors++;
        console.error(`✗ ${userEmail}:`, e.message || e);
      }
    }
  } finally {
    await conn.end();
  }

  console.log('\n--- Resumen g_users → Glide PROFILE ---');
  console.log(`Actualizados / candidatos dry-run: ${updated}`);
  console.log(`Omitidos (profile ya tenía URL o sin cambio): ${skippedHasUrl}`);
  console.log(`No encontrados en Glide profile: ${missingInProfile}`);
  console.log(`Errores: ${errors}`);
}

run().catch(e => {
  console.error(e.message || e);
  process.exit(1);
});
