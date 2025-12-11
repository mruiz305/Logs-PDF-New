require('dotenv').config();
const fs = require('fs');
const path = require('path');
const mysql = require('mysql2/promise');
const puppeteer = require('puppeteer');
const { google } = require('googleapis');
const { sendViaGmail } = require('./gmailSender');

/* ============= CLI ============= */
function argValue(name, def = '') {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 && process.argv[i + 1] ? process.argv[i + 1] : def;
}
function hasFlag(name) {
  return process.argv.includes(`--${name}`);
}
function must(value, label) {
  if (!value) {
    console.error(`Falta el parámetro --${label}`);
    process.exit(1);
  }
  return value;
}

/* ============= MySQL ============= */
async function getMysqlConnection() {
  return mysql.createConnection({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_DATABASE,
    charset: 'utf8mb4',
    dateStrings: true,
  });
}

/* ============= Registro de usuarios sin URL ============= */
/**
 * Ahora reutiliza la MISMA conexión (conn) que ya existe.
 * NO abre ni cierra conexiones nuevas por submitter.
 */
async function registerMissingUrlIfNeeded(conn, submitterName, logDate, origin = 'LOGS', urlToStore = null) {
  if (!submitterName) return;

  // Buscar la fila en stg_g_users para ese submitter
  const [rows] = await conn.execute(
    `
      SELECT logsIndividualFile
      FROM stg_g_users
      WHERE TRIM(name) = TRIM(?)
    `,
    [submitterName]
  );

  if (!rows.length) {
    // No existe → no lo registramos, porque el problema es otro
    return;
  }

  const url = rows[0].logsIndividualFile;

  // Si NO tiene URL → lo registramos en g_users_missing_url
  if (!url || url.trim() === '') {
    await conn.execute(
      `
        INSERT INTO g_users_missing_url (submitter, log_date, origin, log_url)
        VALUES (?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
          log_url   = VALUES(log_url),
          created_at = NOW()
      `,
      [submitterName, logDate, origin, urlToStore]
    );
  }
}

/**
 * Este solo se ejecuta una vez por batch ,
 * así que puede seguir abriendo su propia conexión.
 */
async function sendMissingUrlEmailForDate(logDate, origin = 'LOGS') {
  const conn = await getMysqlConnection();
  try {
    const [rows] = await conn.execute(
      `
      SELECT submitter, log_date, origin
      FROM g_users_missing_url
      WHERE log_date = ?
        AND origin = ?
      ORDER BY submitter
      `,
      [logDate, origin]
    );

    if (!rows.length) {
      // No hay nada que avisar ese día
      return;
    }

    const toAddress = process.env.MISSING_URL_ALERT_TO || 'mruiz@305nofault.com';

    const subject = `Usuarios sin URL en g_users (logsIndividualFile) - ${logDate}`;

    const lista = rows
      .map(r => `- ${r.submitter} (log_date: ${r.log_date})`)
      .join('\n');

    const text = `
Se detectaron usuarios SIN logsIndividualFile en g_users
para la fecha de log ${logDate} y origen ${origin}.

Por favor, revisar la tabla g_users_missing_url en la base de datos
y actualizar la columna logsIndividualFile en g_users para estos usuarios.

Lista de usuarios detectados:
${lista}
    `.trim();

    const mimeBodyLines = [
      `To: ${toAddress}`,
      `Subject: ${subject}`,
      'Content-Type: text/plain; charset=UTF-8',
      '',
      text,
    ];

    const recipientCount = toAddress.split(',').length;

    await sendViaGmail(
      conn,
      'LOGS_MISSING_URL',   // process_code en tblEmailConfig
      mimeBodyLines,
      recipientCount
    );
  } finally {
    await conn.end();
  }
}

/* ============= Queries (reutilizando misma conexión) ============= */

async function fetchLogsForSubmitter(conn, submitterName, from, to) {
  const view = process.env.DB_VIEW_NAME;
  const [rows] = await conn.execute(
    `
      SELECT
        v.name               AS Name,
        v.pipInsurance       AS Insurance,
        v.atfaultInsurance   AS AtFault,
        v.txLocation         AS Locations,
        DATE_FORMAT(v.idot, '%m/%d/%Y') AS IDOT,
        DATE_FORMAT(v.ldot, '%m/%d/%Y') AS LDOT,
        v.Status,
        DATE_FORMAT(v.Signed, '%m/%d/%Y') AS Signed,
        DATE_FORMAT(v.doa, '%m/%d/%Y')    AS DOA,
        v.attorney           AS Attorney,
        v.leadNotes          AS Notes,
        v.Compliance,
        v.convertedValue     AS ConvAtty,
        v.AttyDropReason     AS DropReason,
        v.intakeSpecialist   AS LockedDown,
        DATE(v.dateCameIn)                 AS CameInDate,
        DATE_FORMAT(v.dateCameIn, '%Y-%m') AS CameInYM,
        Confirmed
      FROM ${view} v
      INNER JOIN stg_g_users gu
        ON TRIM(submitter) = TRIM(gu.email)
       AND (
             UPPER(gu.hrStatus) = 'ACTIVE'
             OR (
                  UPPER(gu.hrStatus) = 'TERMED'
                  AND gu.hrTermed >= DATE_SUB(CURDATE(), INTERVAL 6 MONTH)
                )
           )
      WHERE COALESCE(NULLIF(submitterName,''), submitter) = TRIM(?)
        AND DATE(dateCameIn) BETWEEN ? AND ?
      ORDER BY dateCameIn DESC
    `,
    [submitterName, from, to]
  );
  return rows;
}

/* Submitters distintos en el rango */
async function fetchDistinctSubmitters(conn, from, to) {
  const view = process.env.DB_VIEW_NAME;
  const [rows] = await conn.execute(
    `
      SELECT TRIM(COALESCE(NULLIF(v.submitterName,''), v.submitter)) AS submitter
      FROM ${view} v
      INNER JOIN stg_g_users gu
        ON TRIM(submitter) = TRIM(gu.email)
       AND (
             UPPER(gu.hrStatus) = 'ACTIVE'
             OR (
                  UPPER(gu.hrStatus) = 'TERMED'
                  AND gu.hrTermed >= DATE_SUB(CURDATE(), INTERVAL 6 MONTH)
                )
           )
      WHERE DATE(dateCameIn) BETWEEN ? AND ?
        AND COALESCE(NULLIF(v.submitterName,''), v.submitter) IS NOT NULL
      GROUP BY TRIM(COALESCE(NULLIF(v.submitterName,''), v.submitter))
      HAVING submitter <> ''
      ORDER BY submitter
    `,
    [from, to]
  );
  return rows.map(r => r.submitter);
}

/* ============= Utils precisión/normalización ============= */
function todayYMD() {
  const d = new Date();
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

function normalize(s) {
  return (s || '').toString().trim().toUpperCase();
}

/** Suma precisa de dinero (centavos) → string 2 decimales exactos */
function sumMoneyAsString(rows, field) {
  let cents = 0;
  for (const r of rows) {
    const v = (r[field] ?? '').toString().replace(/,/g, '').trim();
    if (v === '') continue;
    const m = /^-?\d+(?:\.(\d{1,2}))?$/.exec(v);
    if (m) {
      const parts = v.split('.');
      const whole = parseInt(parts[0], 10);
      const dec = parts[1] ? parts[1].padEnd(2, '0').slice(0, 2) : '00';
      cents += whole * 100 + (whole < 0 ? -parseInt(dec, 10) : parseInt(dec, 10));
    } else {
      cents += Math.round(parseFloat(v) * 100);
    }
  }
  return (cents / 100).toFixed(2);
}

/* ============= KPIs por grupo con filtro ============= */
function computeStats(rows) {
  const total = rows.length;
  const droppedExact = rows.filter(r => normalize(r.Status) === 'DROPPED').length;
  const droppedAny = rows.filter(r => normalize(r.Status).includes('DROP')).length;
  const pctExactStr = total ? ((droppedExact / total) * 100).toFixed(2) : '0.00';
  const grossLabel = `${total} (${droppedExact} Dropped / ${pctExactStr}%)`;
  return {
    total,
    droppedAny,
    droppedExact,
    pctExactStr,
    grossLabel,
    netValueSumStr: sumMoneyAsString(rows, 'ConvAtty'),
  };
}

function computeKpis(rows) {
  const totalStats = computeStats(rows);

  // REF OUTS
  const refRows = rows.filter(r => {
    const loc = normalize(r.Locations);
    return loc === 'REFERRED OUT' || loc === 'REF OUT';
  });
  const refStats = computeStats(refRows);

  // WORKERS COMP
  const wcRows = rows.filter(r => normalize(r.Locations) === 'WORKERS COMP');
  const wcStats = computeStats(wcRows);

  // COR CASES
  const corRows = rows.filter(r => normalize(r.Locations).startsWith('COR'));
  const corStats = computeStats(corRows);

  return {
    total: totalStats.total,
    droppedAny: totalStats.droppedAny,
    workersCompCount: wcRows.length,
    totalStats,
    refStats,
    wcStats,
    corStats,
  };
}

function monthLabel(ym) {
  // ym viene como 'YYYY-MM'; lo convertimos a 'MMM YYYY' en inglés (ej. 'Nov 2025')
  const [y, m] = ym.split('-').map(Number);
  const d = new Date(y, (m - 1), 1);
  return new Intl.DateTimeFormat('en-US', { month: 'short', year: 'numeric' }).format(d);
}

function computeMonthlyAggregates(rows) {
  const buckets = new Map(); // key: 'YYYY-MM' -> { rows:[] }

  for (const r of rows) {
    const ym = r.CameInYM || (r.CameInDate ? r.CameInDate.toString().slice(0, 7) : null);
    if (!ym) continue;
    if (!buckets.has(ym)) buckets.set(ym, []);
    buckets.get(ym).push(r);
  }

  // Convertimos a arreglo ordenado por mes ascendente
  const months = Array.from(buckets.keys()).sort();
  const agg = [];

  for (const ym of months) {
    const mrows = buckets.get(ym);

    // Reusamos tu lógica de KPIs para cada mes:
    const k = computeKpis(mrows);

    agg.push({
      ym,
      label: monthLabel(ym),
      total: k.total,
      droppedAny: k.droppedAny,
      droppedExact: k.totalStats.droppedExact,
      net: k.totalStats.netValueSumStr,

      refTotal: k.refStats.total,
      refNet: k.refStats.netValueSumStr,
      wcTotal: k.wcStats.total,
      wcNet: k.wcStats.netValueSumStr,
      corTotal: k.corStats.total,
      corNet: k.corStats.netValueSumStr,
    });
  }

  return agg;
}

/* ============= Totales (secciones) ============= */
function buildTotalsLeft(k) {
  return `
  <table class="kpi-left">
    <tbody>
      <tr class="cap">
        <td class="cap-td" colspan="2">
          Bfr: ${k.totalStats.total} (${k.totalStats.droppedAny} Drpd/${((k.totalStats.droppedAny/(k.totalStats.total||1))*100).toFixed(1)}%)
        </td>
      </tr>

      <tr class="sec"><td colspan="2">TOTAL CASES</td></tr>
      <tr><th>NET:</th><td>${k.totalStats.netValueSumStr}</td></tr>
      <tr><th>GROSS CASES:</th><td>${k.totalStats.grossLabel}</td></tr>

      <tr class="sec"><td colspan="2">REF OUTS</td></tr>
      <tr><th>NET:</th><td>${k.refStats.netValueSumStr}</td></tr>
      <tr><th>GROSS CASES:</th><td>${k.refStats.total}</td></tr>

      <tr class="sec"><td colspan="2">WORKERS COMP</td></tr>
      <tr><th>NET:</th><td>${k.wcStats.netValueSumStr}</td></tr>
      <tr><th>GROSS CASES:</th><td>${k.wcStats.total}</td></tr>

      <tr class="sec"><td colspan="2">COR CASES</td></tr>
      <tr><th>NET:</th><td>${k.corStats.netValueSumStr}</td></tr>
      <tr><th>GROSS CASES:</th><td>${k.corStats.total}</td></tr>
    </tbody>
  </table>
  `;
}

/* ============= Tabla principal + colores ============= */
function statusClass(row) {
  const up = (row.Status || '').toUpperCase().trim();

  // Confirmed puede venir como 1/0, '1'/'0', true/false, 'TRUE'
  const confirmed =
    row.Confirmed === 1 ||
    row.Confirmed === true ||
    row.Confirmed === '1' ||
    (typeof row.Confirmed === 'string' && row.Confirmed.toUpperCase() === 'TRUE');

  // 1) DROPPED >60
  if (up.includes('DROPPED') && up.includes('>60')) {
    return 'row-dropped60';
  }

  // 2) Dropped (cualquier DROP sin >60)
  if (up.includes('DROP')) {
    return 'row-dropped';
  }

  // 3) PROBLEM >30
  if (up.includes('PROBLEM') && up.includes('30')) {
    return 'row-problem30';
  }

  // 4) PROBLEM “normal”
  if (up.includes('PROBLEM')) {
    return 'row-problem';
  }

  // 5) Confirmado: por Status o por flag Confirmed
  if (up === 'G' || up.includes('GOOD') || confirmed) {
    return 'row-good';
  }

  // 6) Todo lo demás en blanco
  return 'row-active-blank';
}

function buildGroupedTable(rows) {
  // asume que rows ya vienen ordenadas por dateCameIn DESC (tu query lo hace)
  let lastYM = null;
  let monthIndex = 0;

  const body = rows
    .map(r => {
      const ym = r.CameInYM || (r.CameInDate ? r.CameInDate.toString().slice(0, 7) : '');
      let chunk = '';

      if (ym !== lastYM) {
        // nuevo mes ⇒ encabezado de sección y reinicio de contador
        lastYM = ym;
        monthIndex = 0;
        const label = monthLabel(ym); // 'Mar 2025', 'Apr 2025', ...
        chunk += `
        <tr class="month-row">
          <td class="month-cell" colspan="16">${label.replace(' ', '-')}</td>
        </tr>`;
      }

      monthIndex += 1;

      chunk += `
      <tr class="${statusClass(r)}">
        <td>${monthIndex}</td><td>${r.Name || ''}</td><td>${r.Insurance || ''}</td><td>${r.AtFault || ''}</td>
        <td>${r.Locations || ''}</td><td>${r.IDOT || ''}</td><td>${r.LDOT || ''}</td><td>${r.Status || ''}</td>
        <td>${r.Signed || ''}</td><td>${r.DOA || ''}</td><td>${r.Attorney || ''}</td><td>${r.Notes || ''}</td>
        <td>${r.Compliance || ''}</td><td>${r.ConvAtty || ''}</td><td>${r.DropReason || ''}</td><td>${r.LockedDown || ''}</td>
      </tr>`;
      return chunk;
    })
    .join('');

  return `
    <table class="grid">
      <thead>
        <tr>
          <th>#</th><th>NAME</th><th>INSURANCE</th><th>AT FAULT</th><th>LOCATIONS</th>
          <th>IDOT</th><th>LDOT</th><th>STATUS</th><th>SIGNED</th><th>DOA</th>
          <th>ATTORNEY</th><th>NOTES</th><th>COMPLIANCE</th><th>CONV</th>
          <th>ATTY DROP REASON</th><th>LOCKED DOWN</th>
        </tr>
      </thead>
      <tbody>${body}</tbody>
    </table>
  `;
}

function buildMonthlySummaryTable(monthAgg) {
  if (!monthAgg.length) return '';

  const rows = monthAgg
    .map(
      m => `
    <tr>
      <td>${m.label}</td>
      <td style="text-align:right">${m.total}</td>
      <td style="text-align:right">${m.droppedAny}</td>
      <td style="text-align:right">${m.net}</td>
      <td style="text-align:right">${m.refTotal}</td>
      <td style="text-align:right">${m.refNet}</td>
      <td style="text-align:right">${m.wcTotal}</td>
      <td style="text-align:right">${m.wcNet}</td>
      <td style="text-align:right">${m.corTotal}</td>
      <td style="text-align:right">${m.corNet}</td>
    </tr>
  `
    )
    .join('');

  return `
  <table class="month-summary">
    <thead>
      <tr>
        <th>MONTH</th>
        <th>TOTAL</th>
        <th>DROPPED (any)</th>
        <th>NET</th>
        <th>REF OUTS</th>
        <th>NET (REF)</th>
        <th>WORKERS COMP</th>
        <th>NET (WC)</th>
        <th>COR CASES</th>
        <th>NET (COR)</th>
      </tr>
    </thead>
    <tbody>
      ${rows}
    </tbody>
  </table>`;
}

/* ============= HTML (solo en memoria para PDF) ============= */
function buildHtml(submitterName, rows, from, to) {
  const kpis = computeKpis(rows);
  const monthAgg = computeMonthlyAggregates(rows);
  const reportDate = new Date().toLocaleDateString('en-US');
  const tableHtml = buildGroupedTable(rows);

  return `
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Logs - ${submitterName}</title>
<style>
  @page { size: Letter landscape; margin: 8mm; }
  body  { font-family: Arial, Helvetica, sans-serif; color:#000; font-size:10px; }

  .header-top{
    display:grid;
    grid-template-columns: 22% 48% 30%;
    column-gap:16px;
    align-items:start;
    margin-bottom:4px;
  }

  .kpi-left{
    width: 100%;
    border-collapse: collapse;
    table-layout: fixed;
    font-size: 7.3px;
  }
  .kpi-left th, .kpi-left td{ border:1px solid #000; padding:1.6px 3px; }
  .kpi-left th{ text-align:left; background:#d1e6ff; width:64%; font-weight:700; }
  .kpi-left td{ text-align:right; width:36%; background:#f7f9fb; }
  .kpi-left .sec td{ background:#bcd5ff; font-weight:800; text-align:center; }
  .kpi-left .cap-td{ background:#eef4ff; text-align:right; font-weight:600; color:#34495e; border:1px solid #000; }

  .legend { width:100%; font-size:8.2px; }
  .legend-title{ font-weight:800; margin-bottom:2px; }
  .legend-row{ display:flex; align-items:center; margin:2px 0; }

  .key-tag{
    min-width:70px;
    padding:2px 6px;
    border:1px solid #000;
    border-radius:0;
    font-weight:800;
    font-size:8px;
    text-align:center;
    margin-right:8px;
  }

  /* === COLORES KEY === */
  /* PROBLEM */
  .t-problem{
    background:#FFB3E6;
    color:#000;
  }
  /* PROBLEM >30 */
  .t-problem30{
    background:#FFF2B3;
    color:#000;
  }
  /* DROPPED >60 */
  .t-drop60{
     background:#FFB347;
    color:#000;
  }
  /* Dropped */
  .t-dropped{
    background:#FF9A66;
  }
  /* GOOD!!! */
  .t-good{
    background:#00CC33;
  }

  .who{
    justify-self:end; font-size:8.6px; line-height:1.35; padding:7px 9px;
    border:1px solid #e6eaee; border-radius:8px; background:#fff; box-shadow:0 1px 2px rgba(0,0,0,.03);
    width:max-content; transform: translateX(-24px);
  }
  .who .name{ font-weight:800; font-size:9.4px; margin-bottom:2px; }

  .range-line{ font-size:9px; font-weight:700; margin:6px 0 6px 0; }

  table.grid { width:100%; border-collapse:collapse; table-layout:fixed; }
  table.grid th, table.grid td {
    border:1px solid #000; padding:1.5px 1.6px; text-align:left;
    vertical-align:top; word-wrap:break-word; word-break:break-word;
    font-size:6.9px; line-height:1.03;
  }
  table.grid thead th{
    background:#AFCBE8; color:#0b2a3c; font-size:7.6px; font-weight:800;
    text-align:center; border-top:2px solid #000; border-bottom:2px solid #000;
  }

  table.grid th:nth-child(1){width:3%}
  table.grid th:nth-child(2){width:14%}
  table.grid th:nth-child(3){width:8%}
  table.grid th:nth-child(4){width:9%}
  table.grid th:nth-child(5){width:8%}
  table.grid th:nth-child(6){width:6%}
  table.grid th:nth-child(7){width:6%}
  table.grid th:nth-child(8){width:10%}
  table.grid th:nth-child(9){width:6%}
  table.grid th:nth-child(10){width:6%}
  table.grid th:nth-child(11){width:11%}
  table.grid th:nth-child(12){width:11%}
  table.grid th:nth-child(13){width:4%}
  table.grid th:nth-child(14){width:4%}
  table.grid th:nth-child(15){width:8%}
  table.grid th:nth-child(16){width:10%}

  /* === COLORES FILAS (mismos que la KEY) === */

  .row-problem{
    background:#FFB3E6;
    color:#000;
    font-weight:700;
  }

  .row-problem30{
    background:#FFF2B3;
    color:#000;
    font-weight:700;
  }

  .row-dropped60{
    background:#FFB347;
    color:#000;
    font-weight:700;
  }

  .row-dropped{
    background:#FF9A66;
    color:#000;
    font-weight:700;
  }

  .row-good{
    background:#00CC33;
    color:#000;
    font-weight:700;
  }

  .row-active-blank{
    background:#FFFFFF;
    color:#000;
  }

  table { page-break-inside:auto; break-inside:auto; }
  thead { display:table-header-group; }
  tfoot { display:table-footer-group; }
  tr, td, th { page-break-inside:avoid; break-inside:avoid; }

  .month-summary{
    width:100%; border-collapse:collapse; table-layout:fixed; margin: 8px 0 10px 0;
    font-size:8px;
  }
  .month-summary th, .month-summary td{
    border:1px solid #000; padding:3px 4px;
  }
  .month-summary thead th{
    background:#dfefff; font-weight:800; text-align:center;
  }
  .month-summary td:nth-child(1){ text-align:left; }
  .month-row .month-cell{
    background:#cfe3ff;
    font-weight:800;
    color:#0b2a3c;
    text-align:left;
    padding:4px 6px;
    border:2px solid #000;
    border-left:1px solid #000;
    border-right:1px solid #000;
  }

  .month-row + tr td{
    border-top-width:2px;
  }

  .row-active-blank { background:#FFFFFF; }
</style>
</head>
<body>

  <div class="header-top">
    ${buildTotalsLeft(kpis)}

    <div class="legend">
      <div class="legend-title">KEY:</div>
      <div class="legend-row">
        <span class="key-tag t-problem">PROBLEM</span>
        Case is not compliant &amp; it’s been &gt; 14 days since LDOT
      </div>
      <div class="legend-row">
        <span class="key-tag t-problem30">PROBLEM &gt;30</span>
        Case is not compliant &amp; it’s been &gt; 30 days since LDOT
      </div>
      <div class="legend-row">
        <span class="key-tag t-drop60">DROPPED &gt;60</span>
        Case is not compliant &amp; it’s been &gt; 60 days since LDOT
      </div>
      <div class="legend-row">
        <span class="key-tag t-dropped">Dropped</span>
        Clinic or Atty have indicated the case has dropped. No Credit.
      </div>
      <div class="legend-row">
        <span class="key-tag t-good">GOOD!!!</span>
        Caso Confirmado / Case Confirmed!
      </div>
      <div style="height:3px"></div>
      <div class="legend-row">
        <span class="key-tag t-problem">PROBLEM</span>
        El caso está not compliant y han pasado &gt;14 días desde LDOT
      </div>
      <div class="legend-row">
        <span class="key-tag t-problem30">PROBLEM &gt;30</span>
        El caso está not compliant y han pasado &gt;30 días desde LDOT
      </div>
      <div class="legend-row">
        <span class="key-tag t-drop60">DROPPED &gt;60</span>
        El caso está not compliant y han pasado &gt;60 días desde LDOT
      </div>
      <div class="legend-row">
        <span class="key-tag t-dropped">Dropped</span>
        La clínica o el abogado han indicado que el caso ha sido cerrado. No hay crédito.
      </div>
    </div>

    <div class="who">
      <div class="name">${submitterName}</div>
      <div>From: ${from}</div>
      <div>To: ${to}</div>
      <div>Reported on: ${reportDate}</div>
    </div>
  </div>

  ${tableHtml}
</body>
</html>
  `;
}

/* ============= PDF (reutilizando un solo navegador) ============= */
async function generatePdf(browser, html, outputPath) {
  const page = await browser.newPage();
  try {
    await page.setContent(html, { waitUntil: 'load' });
    await page.emulateMediaType('print');
    await page.pdf({
      path: outputPath,
      format: 'letter',
      landscape: true,
      printBackground: true,
      preferCSSPageSize: true,
      margin: { top: '8mm', right: '8mm', bottom: '8mm', left: '8mm' },
    });
  } finally {
    await page.close();
  }
}

/* ============= Drive (opcional, cliente reutilizable) ============= */

// Cacheamos el cliente de Drive para no recrearlo en cada archivo
let driveClientPromise = null;

async function getDriveClient() {
  if (!driveClientPromise) {
    const auth = new google.auth.GoogleAuth({
      keyFile: process.env.GOOGLE_SERVICE_ACCOUNT_JSON,
      scopes: ['https://www.googleapis.com/auth/drive.file'],
    });
    driveClientPromise = auth.getClient().then(authClient =>
      google.drive({ version: 'v3', auth: authClient })
    );
  }
  return driveClientPromise;
}

async function uploadToDrive(filePath, fileName) {
  const drive = await getDriveClient();
  const folderId = process.env.GOOGLE_DRIVE_FOLDER_ID;

  // 1) Buscar si ya existe un archivo con ese nombre en la carpeta
  const qParts = [`name = '${fileName.replace(/'/g, "\\'")}'`, 'trashed = false'];
  if (folderId) {
    qParts.push(`'${folderId}' in parents`);
  }

  const listRes = await drive.files.list({
    q: qParts.join(' and '),
    fields: 'files(id, name, parents, webViewLink)',
    includeItemsFromAllDrives: true,
    supportsAllDrives: true,
  });

  const existingFile = (listRes.data.files || [])[0];

  const media = {
    mimeType: 'application/pdf',
    body: fs.createReadStream(filePath),
  };

  let fileRes;

  if (existingFile) {
    // 2) Si existe → ACTUALIZAR (reemplazar contenido)
    fileRes = await drive.files.update({
      fileId: existingFile.id,
      media,
      fields: 'id, webViewLink, parents',
      supportsAllDrives: true,
    });

    console.log(`↑ Actualizado en Drive (mismo archivo): ${fileRes.data.webViewLink}`);

    // Ya debería tener permisos; no se vuelve a crear
  } else {
    // 3) Si NO existe → CREAR uno nuevo
    const fileMetadata = {
      name: fileName,
      parents: folderId ? [folderId] : [],
    };

    fileRes = await drive.files.create({
      requestBody: fileMetadata,
      media,
      fields: 'id, webViewLink, parents',
      supportsAllDrives: true,
    });

    // 4) Permiso público solo cuando se crea por primera vez
    await drive.permissions.create({
      fileId: fileRes.data.id,
      requestBody: { role: 'reader', type: 'anyone' },
      supportsAllDrives: true,
    });

    console.log(`↑ Subido a Drive (nuevo archivo): ${fileRes.data.webViewLink}`);
  }

  return fileRes.data;
}

/* ============= Orquestación ============= */
async function generateForOne(conn, browser, submitterName, from, to) {
  const rows = await fetchLogsForSubmitter(conn, submitterName, from, to);
  if (!rows.length) {
    console.log(`(sin datos) ${submitterName}  [${from}..${to}]`);
    return null;
  }

  const html = buildHtml(submitterName, rows, from, to);

  const outDir = process.env.OUTPUT_DIR || path.join(__dirname, 'output');
  if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });

  const base = `Logs_ ${submitterName}`.replace(/[\\/:*?"<>|]+/g, ' ').trim();
  const pdfPath = path.join(outDir, `${base}.pdf`);

  await generatePdf(browser, html, pdfPath);
  console.log(`✔ PDF: ${pdfPath}`);

  // Subida opcional a Drive (por defecto NO sube)
  const shouldUpload = (process.env.UPLOAD_TO_DRIVE || 'false').toLowerCase() === 'true';
  let driveLink = null;

  if (shouldUpload) {
    const up = await uploadToDrive(pdfPath, `${base}.pdf`);
    driveLink = up.webViewLink;

    // reutiliza la MISMA conexión
    await registerMissingUrlIfNeeded(conn, submitterName, to, 'LOGS', driveLink);

    console.log(`↑ Drive: ${driveLink}`);
  } else {
    console.log('↪ UPLOAD_TO_DRIVE=false → no se sube a Drive.');
  }

  return { submitterName, pdfPath, driveLink };
}

/**
 * runBatch: función reutilizable para CLI o cron
 * options:
 *  - from, to: 'YYYY-MM-DD' (opcionales; si faltan ambos, se usa default 2024-01-01..hoy)
 *  - submitter: nombre/correo (opcional)
 *  - runAll: boolean (para todos los submitters)
 */
async function runBatch({ from, to, submitter = '', runAll = false }) {
  let _from = (from || '').trim();
  let _to = (to || '').trim();

  // Si no vienen fechas, usar rango por defecto
  if (!_from && !_to) {
    _from = '2024-01-01';
    _to = todayYMD();
    console.log(`Usando rango por defecto: ${_from} .. ${_to}`);
  } else if (!_from || !_to) {
    // Si se especifica solo una, error
    throw new Error('Si usas from o to, debes especificar ambos (YYYY-MM-DD).');
  }

  if (submitter && runAll) {
    throw new Error('No combines submitter con runAll. Usa uno u otro.');
  }

  const conn = await getMysqlConnection();
  const browser = await puppeteer.launch({
    headless: 'new',
    args: ['--no-sandbox', '--disable-setuid-sandbox'],
  });

  try {
    if (submitter) {
      await generateForOne(conn, browser, submitter, _from, _to);
      return;
    }

    if (runAll) {
      const all = await fetchDistinctSubmitters(conn, _from, _to);
      if (!all.length) {
        console.log('(sin submitters en el rango)');
        return;
      }
      console.log(`Encontrados ${all.length} submitters. Generando...`);

      // Concurrencia configurable por .env
      const concurrency = parseInt(process.env.LOGS_CONCURRENCY || '4', 10);
      let index = 0;

      async function worker() {
        while (index < all.length) {
          const s = all[index++];
          try {
            await generateForOne(conn, browser, s, _from, _to);
          } catch (e) {
            console.error(`Error con ${s}:`, e.message || e);
          }
        }
      }

      const workers = [];
      for (let i = 0; i < concurrency; i++) {
        workers.push(worker());
      }
      await Promise.all(workers);

      console.log('✔ Batch completo.');

      
      // Reactivar el correo de missing URL:
      try {
        await sendMissingUrlEmailForDate(_to, 'LOGS');
        console.log(`✔ Revisión de usuarios sin URL completada para ${_to}.`);
      } catch (e) {
        console.error('Error al enviar correo de usuarios sin URL:', e.message || e);
      }
      
      return;
    }

    console.log('Indica submitter o runAll=true');
  } finally {
    await browser.close();
    await conn.end();
  }
}

/* ============= Entrada CLI ============= */
async function main() {
  const from = argValue('from', '').trim();
  const to = argValue('to', '').trim();
  const submitter = argValue('submitter', '').trim();
  const runAll = hasFlag('all');

  await runBatch({ from, to, submitter, runAll });
}

module.exports = { runBatch };

if (require.main === module) {
  main().catch(e => {
    console.error(e);
    process.exit(1);
  });
}
