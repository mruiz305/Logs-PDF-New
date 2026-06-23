/**
 * Actualiza logsIndividualFile en Glide (misma columna nwYAC que Update URL Logs).
 */
function getGlideTable() {
  const glide = require('@glideapps/tables');
  const token = process.env.GLIDE_TABLE_TOKEN || process.env.GLIDE_TOKEN;
  const app = process.env.GLIDE_APP_ID;
  const tableId = process.env.GLIDE_TABLE_ID;
  if (!token || !app || !tableId) {
    throw new Error(
      'Faltan GLIDE_TABLE_TOKEN (o GLIDE_TOKEN), GLIDE_APP_ID o GLIDE_TABLE_ID en .env'
    );
  }
  return glide.table({
    token,
    app,
    table: tableId,
    columns: {
      email: { type: 'email-address', name: 'e0eY9' },
      logsIndividualFile: { type: 'uri', name: 'nwYAC' },
    },
  });
}

/**
 * @returns {Promise<boolean>} true si se actualizó, false si no se encontró el email
 */
async function updateLogsUrlInGlide(email, logsUrl) {
  const glideTable = getGlideTable();
  const rows = await glideTable.get();
  const existing = rows.find(
    r => (r.email || '').toLowerCase() === (email || '').toLowerCase()
  );
  if (!existing) return false;
  await glideTable.update(existing.$rowID, { logsIndividualFile: logsUrl });
  return true;
}

function isGlideConfigured() {
  return !!(
    (process.env.GLIDE_TABLE_TOKEN || process.env.GLIDE_TOKEN) &&
    process.env.GLIDE_APP_ID &&
    process.env.GLIDE_TABLE_ID
  );
}

module.exports = { getGlideTable, updateLogsUrlInGlide, isGlideConfigured };
