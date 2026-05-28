const { google } = require('googleapis');

let driveClientPromise = null;

async function getDriveClient() {
  if (!driveClientPromise) {
    const auth = new google.auth.GoogleAuth({
      keyFile: process.env.GOOGLE_SERVICE_ACCOUNT_JSON,
      scopes: [
        'https://www.googleapis.com/auth/drive.file',
        'https://www.googleapis.com/auth/drive.readonly',
      ],
    });
    driveClientPromise = auth.getClient().then(authClient =>
      google.drive({ version: 'v3', auth: authClient })
    );
  }
  return driveClientPromise;
}

/**
 * Busca un archivo por nombre en la carpeta de Drive y devuelve su webViewLink.
 * @param {string} fileName - ej. "Logs_ John Doe.pdf"
 */
async function findFileLinkByName(fileName) {
  const drive = await getDriveClient();
  const folderId = process.env.GOOGLE_DRIVE_FOLDER_ID;

  const qParts = [`name = '${fileName.replace(/'/g, "\\'")}'`, 'trashed = false'];
  if (folderId) {
    qParts.push(`'${folderId}' in parents`);
  }

  const listRes = await drive.files.list({
    q: qParts.join(' and '),
    fields: 'files(id, name, webViewLink)',
    includeItemsFromAllDrives: true,
    supportsAllDrives: true,
  });

  const file = (listRes.data.files || [])[0];
  if (!file || !file.webViewLink) return null;

  return { webViewLink: file.webViewLink, id: file.id };
}

module.exports = { getDriveClient, findFileLinkByName };
