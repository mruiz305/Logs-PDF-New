/**
 * CLI: sincroniza logsIndividualFile desde g_users (producción) hacia Glide
 * (tabla MAIN + tabla PROFILE).
 */
require('./loadEnv');
const {
  getProdMysqlConnection,
  gUsersTableName,
} = require('./logsUrlSync');
const {
  isGlideConfigured,
  loadGlideCache,
  clearGlideCache,
  syncLogsUrlToGlide,
  isGlideProfileConfigured,
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
  const profileOnly = hasFlag('profile-only');
  const email = argValue('email', '');

  if (!isGlideConfigured()) {
    throw new Error('Faltan variables GLIDE_TABLE_TOKEN/GLIDE_TOKEN, GLIDE_APP_ID o GLIDE_TABLE_ID.');
  }

  const conn = await getProdMysqlConnection();
  clearGlideCache();
  const cache = await loadGlideCache(true);

  let updatedMain = 0;
  let updatedProfile = 0;
  let skippedHasUrl = 0;
  let missingInGlide = 0;
  let errors = 0;

  try {
    const users = await fetchUsersWithLogsUrl(conn, { all, email });

    console.log(`Usuarios con logsIndividualFile en g_users: ${users.length}`);
    console.log(
      profileOnly
        ? 'Destino: solo Glide PROFILE'
        : 'Destino: Glide MAIN + PROFILE'
    );
    console.log(
      overwrite
        ? 'Modo overwrite: actualiza aunque ya tenga URL.'
        : 'Modo default: solo actualiza celdas vacías en Glide.'
    );
    if (dryRun) console.log('Modo dry-run: no se actualiza Glide.\n');

    for (const user of users) {
      const userEmail = String(user.email || '').trim();
      const url = String(user.logsIndividualFile || '').trim();
      try {
        const mainRow = cache.mainByEmail.get(userEmail.toLowerCase());
        const profileRow = isGlideProfileConfigured()
          ? cache.profileByEmail.get(userEmail.toLowerCase())
          : null;

        if (!profileOnly && !mainRow) {
          missingInGlide++;
          console.warn(`⚠ Email no encontrado en Glide main: ${userEmail}`);
          continue;
        }
        if (profileOnly && !profileRow) {
          missingInGlide++;
          console.warn(`⚠ Email no encontrado en Glide profile: ${userEmail}`);
          continue;
        }

        const currentMainUrl = mainRow
          ? String(mainRow.logsIndividualFile || '').trim()
          : '';
        const currentProfileUrl = profileRow
          ? String(profileRow.logsIndividualFile || '').trim()
          : '';

        const mainNeedsUpdate =
          !profileOnly &&
          mainRow &&
          (overwrite ? currentMainUrl !== url : !currentMainUrl);
        const profileNeedsUpdate =
          profileRow &&
          (overwrite ? currentProfileUrl !== url : !currentProfileUrl);

        if (!mainNeedsUpdate && !profileNeedsUpdate) {
          skippedHasUrl++;
          continue;
        }

        if (dryRun) {
          if (mainNeedsUpdate) {
            console.log(
              `[DRY-RUN main] ${userEmail}: ${currentMainUrl || '(vacío)'} → ${url}`
            );
            updatedMain++;
          }
          if (profileNeedsUpdate) {
            console.log(
              `[DRY-RUN profile] ${userEmail}: ${currentProfileUrl || '(vacío)'} → ${url}`
            );
            updatedProfile++;
          }
          continue;
        }

        const result = await syncLogsUrlToGlide(userEmail, url, {
          main: !profileOnly,
          profile: isGlideProfileConfigured(),
          onlyIfEmpty: !overwrite,
          onlyIfDifferent: true,
          cache,
        });

        if (result.mainUpdated) {
          updatedMain++;
          console.log(`✔ Glide main actualizado: ${userEmail}`);
        }
        if (result.profileUpdated) {
          updatedProfile++;
          console.log(`✔ Glide profile actualizado: ${userEmail}`);
        }
        if (!result.mainUpdated && !result.profileUpdated) {
          skippedHasUrl++;
        }
      } catch (e) {
        errors++;
        console.error(`✗ ${userEmail}:`, e.message || e);
      }
    }
  } finally {
    await conn.end();
  }

  console.log('\n--- Resumen g_users → Glide ---');
  console.log(`Main actualizados / dry-run: ${updatedMain}`);
  console.log(`Profile actualizados / dry-run: ${updatedProfile}`);
  console.log(`Omitidos (ya tenían URL o sin cambio): ${skippedHasUrl}`);
  console.log(`No encontrados en Glide: ${missingInGlide}`);
  console.log(`Errores: ${errors}`);
}

run().catch(e => {
  console.error(e.message || e);
  process.exit(1);
});
