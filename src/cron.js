
require('dotenv').config(); 

const CronManager = require('./utils/cronManager');
const { runBatch } = require('./generateLogsPdf');
const Params = require('./config/env');

function todayYMD() {
  const d = new Date();
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

async function job() {
  try {
    const from = '2024-01-01';   
    const to   = todayYMD();

    await runBatch({ from, to, submitter: '', runAll: true });
    console.log(`[${new Date().toISOString()}] Cron run complete.`);
  } catch (err) {
    console.error('Error during cron job run:', err);
  }
}

(async () => {
  try {
    await job();
    console.log('✔ Primera corrida manual completa.');
  } catch (err) {
    console.error('Error en primera corrida:', err);
  }

  // Cron dinámico leyendo de la BD
  const mgr = new CronManager(Params.PROCESS_NAME_CRON, job, { watchInterval: 30_000 });
  await mgr.start();

  console.log(`Watching cron config for "${Params.PROCESS_NAME_CRON}" every 30s`);
})();
