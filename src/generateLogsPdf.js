require('./loadEnv');
const fs = require('fs');
const path = require('path');
const mysql = require('mysql2/promise');
const puppeteer = require('puppeteer');
const { google } = require('googleapis');
const { sendViaGmail } = require('./gmailSender');
const { syncEnabled, syncLogsUrlForSubmitter, getProdMysqlConnection } = require('./logsUrlSync');
const { CNV_COLOR_SCALE, cnvColorMeta } = require('./cnvColors');
const { buildCoverageMapHtml } = require('./coverageMap');
const { execFile } = require('child_process');
const { promisify } = require('util');
const execFileP = promisify(execFile);

// Si qpdf está instalado por Chocolatey, queda en el PATH → basta "qpdf"
const QPDF_PATH = process.env.QPDF_PATH || 'qpdf';

function puppeteerLaunchOptions() {
  const opts = {
    headless: 'new',
    args: ['--no-sandbox', '--disable-setuid-sandbox'],
  };
  const exe = process.env.PUPPETEER_EXECUTABLE_PATH || process.env.CHROME_PATH;
  if (exe) opts.executablePath = exe;
  return opts;
}

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

async function fetchEligibleRepsFromUsers(conn) {
  const [rows] = await conn.execute(`
    SELECT TRIM(COALESCE(NULLIF(name,''), email)) AS submitter
    FROM stg_g_users
    WHERE systemDepartment = 'Marketing'
      AND UPPER(hrStatus) = 'ACTIVE'
      AND \`rank\` IS NOT NULL
      AND TRIM(COALESCE(NULLIF(name,''), email)) <> ''
    ORDER BY submitter
  `);
  return rows.map(r => r.submitter);
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
  const total = rows.filter(r => normalize(r.Status) !== 'CONFLICT').length;
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

/** Meses calendario que intersectan [fromYmd, toYmd], orden ascendente (YYYY-MM-DD). */
function monthsInclusiveBetween(fromYmd, toYmd) {
  const parse = s => {
    const [y, m] = s.split('-').map(Number);
    return { y, m };
  };
  const a = parse(fromYmd);
  const b = parse(toYmd);
  const start = a.y * 12 + a.m <= b.y * 12 + b.m ? a : b;
  const end = a.y * 12 + a.m <= b.y * 12 + b.m ? b : a;
  const out = [];
  let y = start.y;
  let m = start.m;
  while (y < end.y || (y === end.y && m <= end.m)) {
    out.push(`${y}-${String(m).padStart(2, '0')}`);
    m += 1;
    if (m > 12) {
      m = 1;
      y += 1;
    }
  }
  return out;
}

function earliestCameInYm(rows) {
  let min = null;
  for (const r of rows) {
    const ym = r.CameInYM || (r.CameInDate ? r.CameInDate.toString().slice(0, 7) : '');
    if (!ym) continue;
    if (min === null || ym < min) min = ym;
  }
  return min;
}

/**
 * Meses de la grilla: desde el primer mes con al menos un caso del rep hasta el fin del rango
 * (no se muestran meses vacíos anteriores a su primera actividad en el período).
 */
function monthsForRepGridDesc(fromYmd, toYmd, rows) {
  const asc = monthsInclusiveBetween(fromYmd, toYmd);
  if (!asc.length) return [];
  const rangeStartYm = asc[0];
  const rangeEndYm = asc[asc.length - 1];
  const firstWithData = earliestCameInYm(rows);
  let startYm = firstWithData != null ? firstWithData : rangeStartYm;
  if (startYm < rangeStartYm) startYm = rangeStartYm;
  if (startYm > rangeEndYm) startYm = rangeEndYm;
  return asc.filter(ym => ym >= startYm).reverse();
}

function cameInTimeDesc(r) {
  const d = r.CameInDate;
  if (!d) return 0;
  const t = Date.parse(`${d.toString().slice(0, 10)}T12:00:00`);
  return Number.isNaN(t) ? 0 : t;
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

  // 6) CONFLICT: por Status
  if (up.includes('CONFLICT')) {
    return 'row-conflict';
  }

  // 7) Todo lo demás en blanco
  return 'row-active-blank';
}

function buildGroupedTable(rows, fromYmd, toYmd) {
  const months = monthsForRepGridDesc(fromYmd, toYmd, rows);
  const buckets = new Map();
  const noMonth = [];

  for (const r of rows) {
    const ym = r.CameInYM || (r.CameInDate ? r.CameInDate.toString().slice(0, 7) : '');
    if (!ym) {
      noMonth.push(r);
      continue;
    }
    if (!buckets.has(ym)) buckets.set(ym, []);
    buckets.get(ym).push(r);
  }

  for (const [, mrows] of buckets) {
    mrows.sort((a, b) => cameInTimeDesc(b) - cameInTimeDesc(a));
  }
  noMonth.sort((a, b) => cameInTimeDesc(b) - cameInTimeDesc(a));

  const chunks = [];

  for (const ym of months) {
    const label = monthLabel(ym);
    chunks.push(`
        <tr class="month-row">
          <td class="month-cell" colspan="16">${label.replace(' ', '-')}</td>
        </tr>`);

    const mrows = buckets.get(ym) || [];
    if (!mrows.length) {
      chunks.push(`
      <tr class="row-active-blank">
        <td colspan="16" class="month-empty-msg">No cases recorded for this month.</td>
      </tr>`);
    } else {
      let monthIndex = 0;
      for (const r of mrows) {
        monthIndex += 1;
        chunks.push(`
      <tr class="${statusClass(r)}">
        <td>${monthIndex}</td><td>${r.Name || ''}</td><td>${r.Insurance || ''}</td><td>${r.AtFault || ''}</td>
        <td>${r.Locations || ''}</td><td>${r.IDOT || ''}</td><td>${r.LDOT || ''}</td><td>${r.Status || ''}</td>
        <td>${r.Signed || ''}</td><td>${r.DOA || ''}</td><td>${r.Attorney || ''}</td><td>${r.Notes || ''}</td>
        <td>${r.Compliance || ''}</td><td>${r.ConvAtty || ''}</td><td>${r.DropReason || ''}</td><td>${r.LockedDown || ''}</td>
      </tr>`);
      }
    }
  }

  if (noMonth.length) {
    chunks.push(`
        <tr class="month-row">
          <td class="month-cell" colspan="16">Unknown-date</td>
        </tr>`);
    let monthIndex = 0;
    for (const r of noMonth) {
      monthIndex += 1;
      chunks.push(`
      <tr class="${statusClass(r)}">
        <td>${monthIndex}</td><td>${r.Name || ''}</td><td>${r.Insurance || ''}</td><td>${r.AtFault || ''}</td>
        <td>${r.Locations || ''}</td><td>${r.IDOT || ''}</td><td>${r.LDOT || ''}</td><td>${r.Status || ''}</td>
        <td>${r.Signed || ''}</td><td>${r.DOA || ''}</td><td>${r.Attorney || ''}</td><td>${r.Notes || ''}</td>
        <td>${r.Compliance || ''}</td><td>${r.ConvAtty || ''}</td><td>${r.DropReason || ''}</td><td>${r.LockedDown || ''}</td>
      </tr>`);
    }
  }

  const body = chunks.join('');

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

function htmlEscape(s) {
  return String(s ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function prodStatesCnvViewRef() {
  const v = (process.env.PROD_STATES_CNV_VIEW || 'vw_states_cnv').trim();
  if (!/^[a-zA-Z0-9_.]+$/.test(v)) {
    throw new Error('PROD_STATES_CNV_VIEW inválido');
  }
  return v;
}

function quoteSqlIdent(name) {
  return `\`${String(name).replace(/`/g, '')}\``;
}

/** Resuelve nombres reales de columnas (ej. State, cnv en vw_states_cnv). */
async function resolveStatesCnvColumns(conn, view) {
  const [colRows] = await conn.execute(`SHOW COLUMNS FROM ${view}`);
  const names = colRows.map(c => c.Field);
  if (!names.length) {
    throw new Error(`La vista ${view} no tiene columnas`);
  }

  const pick = (wanted, fallbackFn) => {
    if (wanted && names.includes(wanted)) return wanted;
    if (wanted) {
      const ci = names.find(n => n.toLowerCase() === wanted.toLowerCase());
      if (ci) return ci;
    }
    return fallbackFn();
  };

  const stateCol = pick(process.env.PROD_STATES_CNV_COL_STATE || 'state', () => {
    return (
      names.find(n => n.toLowerCase() === 'state') ||
      names.find(n => /state/i.test(n)) ||
      names[0]
    );
  });

  const cbvCol = pick(process.env.PROD_STATES_CNV_COL_CBV || 'cnv', () => {
    return (
      names.find(n => /^cnv$/i.test(n)) ||
      names.find(n => /^cbv$/i.test(n)) ||
      names.find(n => /cnv|cbv/i.test(n) && n !== stateCol) ||
      names.find(n => n !== stateCol) ||
      names[1]
    );
  });

  return { stateCol, cbvCol, names };
}

/** Vista estados/CNV → filas { state, cnv }. Intenta PROD_DB_* y opcionalmente DB_* como fallback. */
async function fetchStatesCnvRows() {
  const view = prodStatesCnvViewRef();
  const attempts = [];
  const useMainOnly = (process.env.STATES_CNV_USE_MAIN_DB || '').toLowerCase() === 'true';
  const allowMainFallback =
    (process.env.STATES_CNV_FALLBACK_MAIN_DB || 'true').toLowerCase() !== 'false';

  if (useMainOnly) {
    attempts.push({ label: 'DB_*', connect: () => getMysqlConnection() });
  } else {
    attempts.push({ label: 'PROD_DB_*', connect: () => getProdMysqlConnection() });
    if (allowMainFallback) {
      attempts.push({ label: 'DB_*', connect: () => getMysqlConnection() });
    }
  }

  const errors = [];
  for (const { label, connect } of attempts) {
    let conn;
    try {
      conn = await connect();
      const { stateCol, cbvCol } = await resolveStatesCnvColumns(conn, view);
      const [rows] = await conn.execute(
        `SELECT ${quoteSqlIdent(stateCol)} AS state, ${quoteSqlIdent(cbvCol)} AS cnv
         FROM ${view}
         ORDER BY ${quoteSqlIdent(stateCol)}`
      );
      if (rows.length) {
        console.log(`✔ CNV estados: ${rows.length} filas desde ${label} / ${view}`);
      } else {
        console.warn(`⚠ ${view} en ${label} devolvió 0 filas`);
      }
      return rows;
    } catch (e) {
      errors.push(`${label}: ${e.message || e}`);
    } finally {
      if (conn) await conn.end();
    }
  }

  throw new Error(errors.join(' | '));
}

function formatCbvCell(v) {
  if (v === null || v === undefined) return '';
  if (typeof v === 'number') {
    return Number.isFinite(v)
      ? v.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
      : '';
  }
  const s = String(v).trim();
  const n = parseFloat(s.replace(/[$,]/g, ''));
  if (!Number.isNaN(n) && /^[\d$.,\s-]+$/.test(s)) {
    return n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  }
  return s;
}

function buildCnvLegendHtml() {
  const items = CNV_COLOR_SCALE.map(
    e => `<div class="states-cnv-legend-item">
      <span class="states-cnv-swatch" style="background:${e.color}"></span>
      <span>${htmlEscape(e.label)}</span>
    </div>`
  ).join('');
  return `<div class="states-cnv-legend">${items}</div>`;
}

/** Reparte filas en N columnas (relleno vertical: col1 arriba→abajo, luego col2…). */
function splitRowsIntoColumns(rows, columnCount = 3) {
  const n = Math.max(1, columnCount);
  const perCol = Math.ceil(rows.length / n);
  const cols = [];
  for (let c = 0; c < n; c++) {
    cols.push(rows.slice(c * perCol, (c + 1) * perCol));
  }
  return cols;
}

function buildStatesCnvColumnTable(colRows, startIndex = 1) {
  if (!colRows.length) {
    return '<table class="states-cnv-grid"><tbody></tbody></table>';
  }
  const body = colRows
    .map((r, i) => {
      const meta = cnvColorMeta(r.cnv);
      const num = startIndex + i;
      return `<tr class="${i % 2 === 0 ? 'states-row-even' : 'states-row-odd'}">
        <td class="states-col-id">${num}</td>
        <td class="states-col-name">${htmlEscape(r.state)}</td>
        <td class="states-col-cnv" style="background:${meta.color};color:${meta.textColor};font-weight:700">${htmlEscape(formatCbvCell(r.cnv))}</td>
      </tr>`;
    })
    .join('');
  return `
    <table class="states-cnv-grid">
      <thead>
        <tr>
          <th>#</th>
          <th>State</th>
          <th>CNV</th>
        </tr>
      </thead>
      <tbody>${body}</tbody>
    </table>`;
}

function buildStatesCnvAppendixHtml(statesRows) {
  const sorted = [...(statesRows || [])].sort((a, b) =>
    String(a.state || '').localeCompare(String(b.state || ''), 'en', { sensitivity: 'base' })
  );
  const columnCount = parseInt(process.env.STATES_CNV_PDF_COLUMNS || '3', 10);
  const cols = splitRowsIntoColumns(sorted, columnCount);
  const count = sorted.length;

  let rowOffset = 1;
  const columnsHtml = count
    ? cols
        .filter(col => col.length > 0)
        .map(col => {
          const html = `<div class="states-cnv-column">${buildStatesCnvColumnTable(col, rowOffset)}</div>`;
          rowOffset += col.length;
          return html;
        })
        .join('')
    : '<div class="states-cnv-empty">Sin datos CNV disponibles.</div>';

  return `
  <div class="states-cnv-wrap">
    <div class="states-cnv-header">
      <div>
        <div class="states-cnv-title">State Conversion Values (CNV)</div>
        ${buildCnvLegendHtml()}
      </div>
      <div class="states-cnv-meta">${count} states</div>
    </div>
    <div class="states-cnv-columns">${columnsHtml}</div>
  </div>`;
}

/* ============= HTML (solo en memoria para PDF) ============= */
function buildHtml(submitterName, rows, from, to, tableHtmlOverride = null, statesCnvHtml = '') {
  const kpis = computeKpis(rows);
  const reportDate = new Date().toLocaleDateString('en-US');
  const tableHtml = tableHtmlOverride ?? buildGroupedTable(rows, from, to);

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
    background:#ff6961;
  }
  /* GOOD!!! */
  .t-good{
    background:#00CC33;
  }
  /* CONFLICT!!! */
  .t-conflict
  {
    background:#4c007d;
    color:#fff;
  }

  .who{
    justify-self:end; font-size:8.6px; line-height:1.35; padding:7px 9px;
    border:1px solid #e6eaee; border-radius:8px; background:#fff; box-shadow:0 1px 2px rgba(0,0,0,.03);
    width:max-content; transform: translateX(-24px);
  }
  .who .name{ font-weight:800; font-size:9.4px; margin-bottom:2px; }

  .range-line{ font-size:9px; font-weight:700; margin:6px 0 6px 0; }

  .cnv-section{
    page-break-before: always;
    break-before: page;
  }

  .states-cnv-wrap{
    padding-top: 4px;
    margin-top: 2px;
    border-top: 2px solid #0b2a3c;
    page-break-inside: avoid;
    break-inside: avoid;
  }
  .states-cnv-header{
    display: flex;
    justify-content: space-between;
    align-items: flex-end;
    border-bottom: 2px solid #0b2a3c;
    margin-bottom: 4px;
    padding-bottom: 2px;
  }
  .states-cnv-title{
    font-size: 10px;
    font-weight: 800;
    color: #0b2a3c;
    letter-spacing: 0.02em;
  }
  .states-cnv-meta{
    font-size: 9px;
    font-weight: 700;
    color: #0b2a3c;
    background: #e8f1fc;
    border: 1px solid #9bb8d9;
    padding: 3px 10px;
    border-radius: 3px;
  }
  .states-cnv-columns{
    display: grid;
    grid-template-columns: 1fr 1fr 1fr;
    column-gap: 12px;
    align-items: start;
  }
  .states-cnv-column{ min-width: 0; }
  .states-cnv-grid{
    width: 100%;
    border-collapse: collapse;
    font-size: 8.5px;
    table-layout: fixed;
  }
  .states-cnv-grid th,
  .states-cnv-grid td{
    border: 1px solid #b8c5d6;
    padding: 2.5px 5px;
    line-height: 1.15;
  }
  .states-cnv-grid thead th{
    background: #1a5fb4;
    color: #fff;
    font-weight: 700;
    font-size: 8.5px;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    border-color: #0b2a3c;
    padding: 4px 5px;
  }
  .states-col-id{
    text-align: center;
    font-weight: 700;
    color: #555;
    width: 8%;
    font-variant-numeric: tabular-nums;
  }
  .states-col-name{
    text-align: left;
    font-weight: 600;
    color: #1a1a1a;
    width: 52%;
    word-break: break-word;
  }
  .states-col-cnv{
    text-align: right;
    font-variant-numeric: tabular-nums;
    color: #0b2a3c;
    width: 40%;
  }
  .states-row-even td{ background: #fff; }
  .states-row-odd td{ background: #f4f7fb; }
  .states-cnv-legend{
    display: flex;
    flex-wrap: wrap;
    gap: 8px 14px;
    margin-top: 5px;
    font-size: 7px;
    font-weight: 600;
  }
  .states-cnv-legend-item{
    display: flex;
    align-items: center;
    gap: 4px;
  }
  .states-cnv-swatch{
    width: 12px;
    height: 12px;
    border: 1px solid #333;
    border-radius: 2px;
    flex-shrink: 0;
  }
  .states-cnv-empty{
    font-size: 8px;
    color: #666;
    padding: 8px 0;
  }

  .cov-map-wrap{
    background: #0f1419;
    color: #e8eef4;
    padding: 14px 26px 12px;
    margin-top: 0;
    border: 1px solid #1f5660;
    border-radius: 8px;
    -webkit-print-color-adjust: exact;
    print-color-adjust: exact;
  }
  .cov-map-error{
    background: #4a2020;
    border: 1px solid #e53935;
    color: #ffd6d6;
    padding: 6px 8px;
    margin-bottom: 8px;
    font-size: 7px;
    line-height: 1.35;
    border-radius: 3px;
  }
  .cov-map-header{
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 6px;
    border-bottom: 1px solid #2dd4bf;
    padding-bottom: 4px;
  }
  .cov-map-logo{
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    gap: 3px;
  }
  .cov-map-logo-img{
    width: 290px;
    height: auto;
    display: block;
    object-fit: contain;
    transform: translateY(3px);
  }
  .cov-map-mark{
    display: flex;
    align-items: flex-end;
    gap: 4px;
    height: 32px;
  }
  .cov-map-mark span{
    width: 18px;
    background: linear-gradient(180deg, #2dd4e4 0%, #1190a0 100%);
    border-radius: 1px 1px 0 0;
  }
  .cov-map-mark span:nth-child(1){ height: 16px; }
  .cov-map-mark span:nth-child(2){ height: 24px; }
  .cov-map-mark span:nth-child(3){ height: 32px; }
  .cov-map-brand{
    color: #ffffff;
    font-family: Arial Black, Arial, sans-serif;
    font-size: 18px;
    font-weight: 900;
    letter-spacing: 0.08em;
    line-height: 1;
    border-top: 1px solid rgba(255,255,255,0.85);
    border-bottom: 1px solid rgba(255,255,255,0.85);
    padding: 2px 0 3px;
  }
  .cov-map-brand-sub{
    font-size: 6.5px;
    color: #8aa4b8;
    letter-spacing: 0.14em;
    margin-top: 1px;
  }
  .cov-map-heading{ text-align: center; }
  .cov-map-title{ font-size: 20px; font-weight: 900; color: #f3f6f8; letter-spacing: 0.1em; line-height: 1.05; }
  .cov-map-date{ font-size: 10px; color: #eef4f7; margin-top: 2px; font-weight: 800; letter-spacing: 0.1em; }
  .cov-map-country{ font-size: 8px; color: #eef4f7; margin-top: 6px; font-weight: 800; letter-spacing: 0.16em; }
  .cov-map-body{
    width: 100%;
  }
  .cov-map-canvas{
    background: #151b22;
    border: 1px solid #2d8891;
    border-radius: 7px;
    padding: 12px 16px;
  }
  .cov-map-stage{
    position: relative;
    height: 452px;
    overflow: hidden;
  }
  .cov-map-svg{ width: 100%; height: 452px; display: block; }
  .cov-map-footer{
    margin-top: 7px;
    padding-top: 7px;
    border-top: 1px solid rgba(45,212,191,0.45);
  }
  .cov-footer-regions-block{
    background: #151b22;
    border: 1px solid #2d8891;
    border-radius: 6px;
    padding: 7px 10px;
  }
  .cov-map-insets{
    position: absolute;
    left: 2px;
    bottom: 18px;
    display: flex;
    flex-direction: column;
    gap: 8px;
    width: 114px;
    pointer-events: none;
  }
  .cov-map-inset{
    background: rgba(15, 20, 25, 0.72);
    border: 1px solid rgba(45, 212, 191, 0.65);
    border-radius: 6px;
    padding: 5px 5px 4px;
    text-align: center;
  }
  .cov-map-inset svg{ width: 100%; height: 66px; display: block; }
  .cov-map-inset-label{
    color: #dbeafe;
    font-size: 7px;
    font-weight: 700;
    letter-spacing: 0.03em;
    line-height: 1;
    margin-top: 1px;
  }
  .cov-state-inset{ stroke-width: 1.2; }
  .cov-state{
    stroke: rgba(226,232,240,0.7);
    stroke-width: 0.7;
  }
  .cov-state-label-internal{
    font-family: Arial, Helvetica, sans-serif;
    font-weight: 600;
    letter-spacing: 0;
    pointer-events: none;
  }
  .cov-state-label-callout-text{
    font-family: Arial, Helvetica, sans-serif;
    font-weight: 600;
    fill: #e8eef4;
    letter-spacing: 0.01em;
    pointer-events: none;
  }
  .cov-label-line{
    stroke: rgba(241, 245, 249, 0.65);
    stroke-width: 0.8;
    fill: none;
    pointer-events: none;
  }
  .cov-dc-marker{ pointer-events: none; }
  .cov-map-banner{
    position: absolute;
    left: 22%;
    right: 31%;
    bottom: 16px;
    min-height: 38px;
    border: 1px solid rgba(148, 163, 184, 0.75);
    border-radius: 5px;
    background: rgba(15, 20, 25, 0.72);
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 12px;
    color: #2dd4bf;
    text-align: left;
  }
  .cov-map-check{
    width: 27px;
    height: 27px;
    border: 2px solid #2dd4bf;
    border-radius: 50%;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 20px;
    font-weight: 800;
    line-height: 1;
  }
  .cov-map-banner-title{
    font-size: 13px;
    font-weight: 900;
    letter-spacing: 0.04em;
    line-height: 1.1;
  }
  .cov-map-banner-subtitle{
    color: #e8eef4;
    font-size: 8px;
    font-weight: 600;
    margin-top: 2px;
  }
  .cov-map-overlay-legend{
    position: absolute;
    right: 16px;
    bottom: 18px;
    min-width: 148px;
    border: 1px solid rgba(45, 212, 191, 0.55);
    border-radius: 5px;
    background: rgba(15, 20, 25, 0.88);
    padding: 7px 9px;
  }
  .cov-map-overlay-item{
    display: flex;
    align-items: center;
    gap: 7px;
    color: #f1f5f9;
    font-size: 8.5px;
    font-weight: 600;
    line-height: 1.3;
  }
  .cov-map-overlay-item + .cov-map-overlay-item{ margin-top: 5px; }
  .cov-map-overlay-item span:first-child{
    width: 13px;
    height: 10px;
    border: 1px solid rgba(255,255,255,0.4);
    flex-shrink: 0;
  }
  .cov-legend-title{ font-weight: 800; color: #2dd4bf; margin-bottom: 8px; font-size: 11px; text-transform: uppercase; letter-spacing: 0.05em; }
  .cov-legend{ display: flex; flex-direction: column; gap: 5px; }
  .cov-legend-item{ display: flex; align-items: center; gap: 8px; font-size: 10px; }
  .cov-legend-swatch{ width: 18px; height: 13px; border: 1px solid rgba(255,255,255,0.35); flex-shrink: 0; }
  .cov-map-stats{
    display: flex;
    flex-direction: column;
    gap: 3px;
    margin-top: 10px;
    padding-top: 8px;
    border-top: 1px solid #2a3540;
    font-size: 10px;
    line-height: 1.4;
  }
  .cov-map-stats strong{ color: #2dd4bf; }
  .cov-footer-title{
    font-size: 9px;
    font-weight: 700;
    color: #2dd4bf;
    margin-bottom: 6px;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    line-height: 1.2;
  }
  .cov-regions{
    display: flex;
    flex-wrap: wrap;
    gap: 5px;
    font-size: 8px;
  }
  .cov-region{
    flex: 1;
    min-width: 120px;
    background: #0f1419;
    border: 1px solid #2a3540;
    border-radius: 4px;
    padding: 5px 7px;
  }
  .cov-region::before{
    content: none;
  }
  .cov-region-title{ font-weight: 700; color: #ff9800; margin-bottom: 2px; font-size: 8px; }
  .cov-region-states{ color: #dce8f0; line-height: 1.3; font-size: 7.5px; }
  .cov-region-empty{ font-size: 7.5px; color: #8aa4b8; }

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
    background:#ff6961;
    color:#000;
    font-weight:700;
  }

  .row-good{
    background:#00CC33;
    color:#000;
    font-weight:700;
  }

    .row-conflict{
    background:#4c007d;
    color:#fff;
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

  .month-empty-msg{
    text-align:center;
    font-style:italic;
    color:#444;
    padding:8px 6px !important;
    font-size:7.5px;
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
        <span class="key-tag t-conflict">CONFLICT</span>
        This case is in conflict with another case.
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
      <div class="legend-row">
        <span class="key-tag t-conflict">CONFLICT</span>
        El caso esta en conflicto con otro caso
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

  ${statesCnvHtml}
</body>
</html>
  `;
}

/* ============= PDF (reutilizando un solo navegador) ============= */
async function generatePdf(browser, html, outputPath) {
  const page = await browser.newPage();
  try {
    page.setDefaultTimeout(0);
    page.setDefaultNavigationTimeout(0);

    await page.setContent(html, { waitUntil: 'load' });
    await page.emulateMediaType('print');
    // Espera breve para que el SVG del mapa termine de layout antes del PDF multipágina
    await page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));
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

async function linearizePdf(pdfPath) {
  const tmp = pdfPath.replace(/\.pdf$/i, '.linear.pdf');

  await execFileP(QPDF_PATH, [
    '--linearize',
    pdfPath,
    tmp
  ]);

  fs.renameSync(tmp, pdfPath);
}

/* ============= Orquestación ============= */
async function generateForOne(conn, browser, submitterName, from, to, options = {}) {
  const rows = await fetchLogsForSubmitter(conn, submitterName, from, to);
  if (!rows.length) {
    console.log(`(sin datos) ${submitterName}  [${from}..${to}]`);
  }

  const hasRows = rows && rows.length > 0;
  const tableHtml = hasRows
    ? buildGroupedTable(rows, from, to)
    : `<div style="padding:10px; border:1px solid #000; font-size:10px;">
       No cases found for this rep in the selected date range.
     </div>`;

  let statesCnvHtml = '';
  let statesRows = [];
  let statesCnvWarning = '';
  try {
    statesRows = await fetchStatesCnvRows();
  } catch (e) {
    statesCnvWarning = `No se pudieron cargar datos de ${prodStatesCnvViewRef()}. ${e.message || e}. Mapa con CNV 0 (plomo) por defecto.`;
    console.warn(`⚠ Mapa / states CNV:`, e.message || e);
  }

  statesCnvHtml = `<div class="cnv-section">${buildCoverageMapHtml(statesRows, new Date(), statesCnvWarning)}`;
  if ((process.env.STATES_CNV_TABLE_IN_PDF || 'true').toLowerCase() === 'true') {
    statesCnvHtml += buildStatesCnvAppendixHtml(statesRows);
  }
  statesCnvHtml += '</div>';

  const html = buildHtml(submitterName, rows, from, to, tableHtml, statesCnvHtml);

  const outDir = process.env.OUTPUT_DIR || path.join(__dirname, 'output');
  if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });

  const base = `Logs_ ${submitterName}`.replace(/[\\/:*?"<>|]+/g, ' ').trim();
  const pdfPath = path.join(outDir, `${base}.pdf`);

  await generatePdf(browser, html, pdfPath);
  console.log(`✔ PDF: ${pdfPath}`);

  await linearizePdf(pdfPath);
  console.log('✔ PDF linearizado (mejor preview en Drive/iPhone)');

  const forceUpload = options.forceUpload === true;
  const shouldUpload =
    !options.skipUpload &&
    (forceUpload || (process.env.UPLOAD_TO_DRIVE || 'false').toLowerCase() === 'true');
  let driveLink = null;

  if (shouldUpload) {
    const up = await uploadToDrive(pdfPath, `${base}.pdf`);
    driveLink = up.webViewLink;
    await registerMissingUrlIfNeeded(conn, submitterName, to, 'LOGS', driveLink);
    console.log(`↑ Drive: ${driveLink}`);
  } else {
    console.log('↪ UPLOAD_TO_DRIVE=false → no se sube a Drive.');
  }

  let urlSyncStatus = 'skipped';
  if (!options.skipUrlSync && syncEnabled()) {
    console.log(`↪ Verificando URL (Drive → producción ${process.env.PROD_DB_DATABASE || 'dbProduction'}/Glide)… ${submitterName}`);
    try {
      urlSyncStatus = await syncLogsUrlForSubmitter(conn, submitterName, { driveLink });
      if (urlSyncStatus === 'unchanged') {
        console.log(`  ✔ URL ya correcta: ${submitterName}`);
      } else if (urlSyncStatus === 'error') {
        console.warn(`  ⚠ URL sync no completada para ${submitterName} (el PDF sí se generó).`);
      }
    } catch (e) {
      console.warn(`  ⚠ URL sync falló (el PDF ya se generó): ${e.message || e}`);
      urlSyncStatus = 'error';
    }
  }

  return { submitterName, pdfPath, driveLink, urlSyncStatus };
}

/**
 * runBatch: función reutilizable para CLI o cron
 * options:
 *  - from, to: 'YYYY-MM-DD' (opcionales; si faltan ambos, se usa default 2024-01-01..hoy)
 *  - submitter: nombre/correo (opcional)
 *  - runAll: boolean (para todos los submitters)
 */
async function runBatch({ from, to, submitter = '', runAll = false, skipUpload = false, skipUrlSync = false }) {
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
  const browser = await puppeteer.launch(puppeteerLaunchOptions());

  const genOpts = { skipUpload, skipUrlSync };

  try {
    if (submitter) {
      await generateForOne(conn, browser, submitter, _from, _to, genOpts);
      return;
    }

    if (runAll) {
//      const all = await fetchDistinctSubmitters(conn, _from, _to);

      const fromCases = await fetchDistinctSubmitters(conn, _from, _to);
      const eligible = await fetchEligibleRepsFromUsers(conn);

      const all = Array.from(new Set([...eligible, ...fromCases])).sort();

      if (!all.length) {
        console.log('(sin submitters en el rango)');
        return;
      }
  
      console.log(`Encontrados ${all.length} submitters (incluye elegibles sin casos). Generando...`);
    
      console.log(`Encontrados ${all.length} submitters. Generando...`);

      // Concurrencia configurable por .env
      const concurrency = parseInt(process.env.LOGS_CONCURRENCY || '4', 10);
      let index = 0;

      async function worker() {
        while (index < all.length) {
          const s = all[index++];
          try {
            await generateForOne(conn, browser, s, _from, _to, genOpts);
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

      console.log('✔ Batch completo (cada PDF incluye verificación de URL si SYNC_URLS_AFTER_PDF≠false).');

      
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
  const skipUpload = hasFlag('no-upload');
  const skipUrlSync = hasFlag('no-sync') || skipUpload;

  if (skipUpload) {
    console.log('↪ Modo prueba: --no-upload (no sube a Drive ni sincroniza URLs).');
  }

  await runBatch({ from, to, submitter, runAll, skipUpload, skipUrlSync });
}

module.exports = {
  runBatch,
  generateForOne,
  puppeteerLaunchOptions,
  todayYMD,
  buildCoverageMapHtml,
};

if (require.main === module) {
  main().catch(e => {
    console.error(e);
    process.exit(1);
  });
}
