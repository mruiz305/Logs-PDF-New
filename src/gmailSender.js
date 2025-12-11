
const { google } = require("googleapis");

async function sendViaGmail(conn, processCode, mimeBodyLines, recipientCount, leadsInfo = [], leadId = null) {
    const cfg = await pickEmailConfig(conn, processCode, recipientCount);

    const toLine = mimeBodyLines.find((l) => l.startsWith("To: ")) || "";
    const ccLine = mimeBodyLines.find((l) => l.startsWith("Cc: ")) || "";
    const bccLine = mimeBodyLines.find((l) => l.startsWith("Bcc: ")) || "";
    const rawSubjLine = mimeBodyLines.find((l) => l.startsWith("Subject: ")) || "";
    const originalSubject = rawSubjLine.slice(9).trim();
    const subjectHeader = `Subject: ${encodeSubject(originalSubject)}`;
    const contentTypeLine =
        mimeBodyLines.find((l) => l.startsWith("Content-Type:")) ||
        "Content-Type: text/html; charset=UTF-8";

    const bodyLines = mimeBodyLines.filter(
        (l) =>
            !l.startsWith("To: ") &&
            !l.startsWith("Cc: ") &&
            !l.startsWith("Bcc: ") &&
            !l.startsWith("Subject: ") &&
            !l.startsWith("Content-Type:") &&
            !l.startsWith("MIME-Version:")
    );

    const headerLines = [
        `From: 1-800-NO-FAULT <${cfg.from_address}>`,
        toLine,
        ccLine,
        bccLine,
        subjectHeader,
        "MIME-Version: 1.0",
        contentTypeLine,
        "Content-Transfer-Encoding: 7bit",
    ].filter((line) => line && line.trim());

    const rawLines = [...headerLines, "", ...bodyLines];

    const raw = Buffer.from(rawLines.join("\r\n"), "utf8")
        .toString("base64")
        .replace(/\+/g, "-")
        .replace(/\//g, "_")
        .replace(/=+$/, "");

    const oa2 = new google.auth.OAuth2(
        cfg.oauth_client_id,
        cfg.oauth_client_secret,
        cfg.redirect_uri || null
    );
    oa2.setCredentials({
        access_token: cfg.access_token,
        refresh_token: cfg.refresh_token,
        scope: cfg.scope,
        expiry_date: cfg.expiry_date,
        token_type: "Bearer",
    });

    const gmail = google.gmail({ version: "v1", auth: oa2 });
    const res = await gmail.users.messages.send({
        userId: "me",
        requestBody: { raw },
    });

    await bumpEmailUsage(conn, cfg.config_id, recipientCount);
}

async function pickEmailConfig(conn, processCode, recipientCount) {
    const configs = await loadEmailConfigs(conn, processCode);
    for (const cfg of configs) {
        const usage = await getEmailUsage(conn, cfg.config_id);
        if (
            usage.sent_count < cfg.daily_message_threshold &&
            usage.recipient_count + recipientCount <= cfg.daily_recipient_threshold
        ) {
            return cfg;
        }
    }
    throw new Error(`No email account available for process ${processCode}`);
}

async function loadEmailConfigs(conn, processCode) {
    const [rows] = await conn.execute(
        `SELECT
        c.config_id,
        e.email_address      AS from_address,
        e.oauth_client_id,
        e.oauth_client_secret,
        e.access_token,
        e.refresh_token,
        e.scope,
        e.expiry_date,
        c.daily_message_threshold,
        c.daily_recipient_threshold
     FROM tblEmailConfig c
     JOIN tblEmail e ON e.email_id = c.email_id
     WHERE c.process_code = ? AND c.is_active = 1
     ORDER BY c.account_type = 'primary' DESC, c.config_id`,
        [processCode]
    );
    return rows;
}

async function getEmailUsage(conn, configId) {
    const [rows] = await conn.execute(
        `SELECT sent_count, recipient_count
       FROM tblEmailUsage
      WHERE config_id = ? AND usage_date = CURDATE()`,
        [configId]
    );
    return rows.length ? rows[0] : { sent_count: 0, recipient_count: 0 };
}

function encodeSubject(subject) {
    return `=?UTF-8?B?${Buffer.from(subject, 'utf8').toString('base64')}?=`;
}

async function bumpEmailUsage(conn, configId, recipientCount) {
    await conn.execute(
        `INSERT INTO tblEmailUsage
       (config_id, usage_date, sent_count, recipient_count)
     VALUES (?, CURDATE(), 1, ?)
     ON DUPLICATE KEY UPDATE
       sent_count      = sent_count + 1,
       recipient_count = recipient_count + VALUES(recipient_count);`,
        [configId, recipientCount]
    );
}

module.exports = {
    sendViaGmail,
};
