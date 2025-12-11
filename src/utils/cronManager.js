const cron = require('node-cron');
const { getConnection } = require('../config/db');

class CronManager {
  constructor(scriptCode, taskFn, opts = {}) {
    this.scriptCode    = scriptCode;
    this.taskFn        = taskFn;
    this.watchInterval = opts.watchInterval || 60_000;
    this.currentExpr   = null;
    this.task          = null;
    this.poller        = null;
  }

  async _fetchConfig() {
    const conn = await getConnection();
    try {
      const [rows] = await conn.execute(
        `SELECT c.expression
           FROM tblCronConfig cc
           JOIN tblCron c ON cc.cron_config_id = c.id
          WHERE cc.script_code = ?
            AND c.is_active    = 1
          LIMIT 1`,
        [this.scriptCode]
      );
      return rows[0] || null;
    } finally {
      conn.release();
    }
  }

  async _reloadIfNeeded() {
    const cfg = await this._fetchConfig();
    if (!cfg) {
      console.warn(`No active cron config for "${this.scriptCode}"`);
      return;
    }
    if (cfg.expression === this.currentExpr) return;

    if (this.task) {
      console.log(`⟳ Updating "${this.scriptCode}" schedule: ${this.currentExpr} → ${cfg.expression}`);
      this.task.destroy();
    } else {
      console.log(`✓ Scheduling "${this.scriptCode}" at ${cfg.expression}`);
    }

    this.task = cron.schedule(cfg.expression, this.taskFn, { scheduled: true });
    this.currentExpr = cfg.expression;
  }

  async start() {
    await this._reloadIfNeeded();
    this.poller = setInterval(() => {
      this._reloadIfNeeded().catch(err =>
        console.error(`Error reloading cron for "${this.scriptCode}":`, err)
      );
    }, this.watchInterval);
  }

  stop() {
    clearInterval(this.poller);
    if (this.task) this.task.destroy();
  }
}

module.exports = CronManager;
