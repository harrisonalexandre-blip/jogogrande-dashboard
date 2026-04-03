/**
 * api/trafego-btag.js
 * Consulta ClickHouse direto para dados do BTAG "Trafego Direto - Meta ads" (aff_id=524865)
 *
 * Modos:
 *   GET /api/trafego-btag?date=YYYY-MM-DD          → single date
 *   GET /api/trafego-btag?from=YYYY-MM-DD&to=YYYY-MM-DD → range (batch, 4 queries)
 *
 * Retorna (single): { ok, date, cadastros, ftds, ftdAmt, depAmt, wdAmt, netDep }
 * Retorna (batch):  { ok, data: [ { date, cadastros, ftds, ftdAmt, depAmt, wdAmt, netDep }, ... ] }
 */

const https = require('https');

const CH_HOST = 'analytics.phoenix365-prod.com';
const CH_USER = 'project_176_ro';
const CH_PASS = 'uChiThoocahtiek9';

const BTAG_FILTER = `position(btag, 'aff_id=524865') > 0 AND is_test = false`;

function queryClickHouse(sql) {
  return new Promise((resolve, reject) => {
    const params = new URLSearchParams({
      query: sql,
      default_format: 'JSON',
      user: CH_USER,
      password: CH_PASS,
    });
    const options = {
      hostname: CH_HOST,
      port: 443,
      path: '/?' + params.toString(),
      method: 'GET',
    };
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try { resolve(JSON.parse(data)); }
        catch (e) { reject(new Error('Parse error: ' + data.slice(0, 200))); }
      });
    });
    req.on('error', reject);
    req.setTimeout(20000, () => { req.destroy(); reject(new Error('timeout')); });
    req.end();
  });
}

// ── Batch: 4 queries com GROUP BY date cobrindo todo o range ─────────────
async function fetchRange(from, to) {
  const sqlCad = `
    SELECT
      toDate(toTimezone(registration_time, 'America/Sao_Paulo')) AS dt,
      count() AS cadastros
    FROM project_176.client
    WHERE ${BTAG_FILTER}
      AND toDate(toTimezone(registration_time, 'America/Sao_Paulo')) >= '${from}'
      AND toDate(toTimezone(registration_time, 'America/Sao_Paulo')) <= '${to}'
    GROUP BY dt ORDER BY dt
  `;

  const sqlFtd = `
    SELECT
      fd.first_dep_date AS dt,
      count() AS ftds,
      sum(d.amount) / 100 AS ftdAmt
    FROM project_176.deposits d
    INNER JOIN (
      SELECT id FROM project_176.client WHERE ${BTAG_FILTER}
    ) c ON c.id = d.client_id
    INNER JOIN (
      SELECT client_id, min(toDate(toTimezone(created_at, 'America/Sao_Paulo'))) AS first_dep_date
      FROM project_176.deposits WHERE status = 'PAID'
      GROUP BY client_id
    ) fd ON fd.client_id = d.client_id
    WHERE d.status = 'PAID'
      AND fd.first_dep_date >= '${from}'
      AND fd.first_dep_date <= '${to}'
      AND toDate(toTimezone(d.created_at, 'America/Sao_Paulo')) = fd.first_dep_date
    GROUP BY dt ORDER BY dt
  `;

  const sqlDep = `
    SELECT
      toDate(toTimezone(d.created_at, 'America/Sao_Paulo')) AS dt,
      sum(d.amount) / 100 AS depAmt
    FROM project_176.deposits d
    INNER JOIN (
      SELECT id FROM project_176.client WHERE ${BTAG_FILTER}
    ) c ON c.id = d.client_id
    WHERE d.status = 'PAID'
      AND toDate(toTimezone(d.created_at, 'America/Sao_Paulo')) >= '${from}'
      AND toDate(toTimezone(d.created_at, 'America/Sao_Paulo')) <= '${to}'
    GROUP BY dt ORDER BY dt
  `;

  const sqlWd = `
    SELECT
      toDate(toTimezone(w.created_at, 'America/Sao_Paulo')) AS dt,
      sum(w.amount) / 100 AS wdAmt
    FROM project_176.withdrawals w
    INNER JOIN (
      SELECT id FROM project_176.client WHERE ${BTAG_FILTER}
    ) c ON c.id = w.client_id
    WHERE w.status = 'PAID'
      AND toDate(toTimezone(w.created_at, 'America/Sao_Paulo')) >= '${from}'
      AND toDate(toTimezone(w.created_at, 'America/Sao_Paulo')) <= '${to}'
    GROUP BY dt ORDER BY dt
  `;

  const [rCad, rFtd, rDep, rWd] = await Promise.all([
    queryClickHouse(sqlCad),
    queryClickHouse(sqlFtd),
    queryClickHouse(sqlDep),
    queryClickHouse(sqlWd),
  ]);

  // Index by date
  const cadByDate  = {}; (rCad.data  || []).forEach(r => { cadByDate[r.dt]  = parseInt(r.cadastros || 0); });
  const ftdByDate  = {}; (rFtd.data  || []).forEach(r => { ftdByDate[r.dt]  = { ftds: parseInt(r.ftds || 0), ftdAmt: parseFloat(r.ftdAmt || 0) }; });
  const depByDate  = {}; (rDep.data  || []).forEach(r => { depByDate[r.dt]  = parseFloat(r.depAmt || 0); });
  const wdByDate   = {}; (rWd.data   || []).forEach(r => { wdByDate[r.dt]   = parseFloat(r.wdAmt  || 0); });

  // Generate list of all dates in range
  const result = [];
  const cur = new Date(from + 'T12:00:00Z');
  const end = new Date(to   + 'T12:00:00Z');
  while (cur <= end) {
    const dt = cur.toISOString().slice(0, 10);
    const cad    = cadByDate[dt]  || 0;
    const ftdRow = ftdByDate[dt]  || { ftds: 0, ftdAmt: 0 };
    const depAmt = depByDate[dt]  || 0;
    const wdAmt  = wdByDate[dt]   || 0;
    const netDep = Math.round((depAmt - wdAmt) * 100) / 100;
    if (cad > 0 || ftdRow.ftds > 0 || depAmt > 0) {
      result.push({
        date:       dt,
        cadastros:  cad,
        ftds:       ftdRow.ftds,
        ftdAmt:     Math.round(ftdRow.ftdAmt * 100) / 100,
        depAmt:     Math.round(depAmt * 100) / 100,
        wdAmt:      Math.round(wdAmt  * 100) / 100,
        netDep,
      });
    }
    cur.setUTCDate(cur.getUTCDate() + 1);
  }
  return result;
}

// ── Single date ───────────────────────────────────────────────────────────
async function fetchSingle(date) {
  const sqlCad = `
    SELECT count() as cadastros
    FROM project_176.client
    WHERE ${BTAG_FILTER}
      AND toDate(toTimezone(registration_time, 'America/Sao_Paulo')) = '${date}'
  `;
  const sqlFtd = `
    SELECT count() as ftds, sum(d.amount) / 100 as ftdAmt
    FROM project_176.deposits d
    INNER JOIN (SELECT id FROM project_176.client WHERE ${BTAG_FILTER}) c ON c.id = d.client_id
    INNER JOIN (
      SELECT client_id, min(toDate(toTimezone(created_at, 'America/Sao_Paulo'))) as first_dep_date
      FROM project_176.deposits WHERE status = 'PAID' GROUP BY client_id
    ) fd ON fd.client_id = d.client_id
    WHERE d.status = 'PAID'
      AND fd.first_dep_date = '${date}'
      AND toDate(toTimezone(d.created_at, 'America/Sao_Paulo')) = '${date}'
  `;
  const sqlDep = `
    SELECT sum(d.amount) / 100 as depAmt
    FROM project_176.deposits d
    INNER JOIN (SELECT id FROM project_176.client WHERE ${BTAG_FILTER}) c ON c.id = d.client_id
    WHERE d.status = 'PAID'
      AND toDate(toTimezone(d.created_at, 'America/Sao_Paulo')) = '${date}'
  `;
  const sqlWd = `
    SELECT sum(w.amount) / 100 as wdAmt
    FROM project_176.withdrawals w
    INNER JOIN (SELECT id FROM project_176.client WHERE ${BTAG_FILTER}) c ON c.id = w.client_id
    WHERE w.status = 'PAID'
      AND toDate(toTimezone(w.created_at, 'America/Sao_Paulo')) = '${date}'
  `;

  const [rCad, rFtd, rDep, rWd] = await Promise.all([
    queryClickHouse(sqlCad), queryClickHouse(sqlFtd),
    queryClickHouse(sqlDep), queryClickHouse(sqlWd),
  ]);

  const cadastros = parseInt(rCad.data?.[0]?.cadastros || 0);
  const ftds      = parseInt(rFtd.data?.[0]?.ftds      || 0);
  const ftdAmt    = parseFloat(rFtd.data?.[0]?.ftdAmt  || 0);
  const depAmt    = parseFloat(rDep.data?.[0]?.depAmt  || 0);
  const wdAmt     = parseFloat(rWd.data?.[0]?.wdAmt    || 0);
  const netDep    = depAmt - wdAmt;

  return { date, cadastros, ftds,
    ftdAmt: Math.round(ftdAmt * 100) / 100,
    depAmt: Math.round(depAmt * 100) / 100,
    wdAmt:  Math.round(wdAmt  * 100) / 100,
    netDep: Math.round(netDep * 100) / 100,
  };
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'GET') return res.status(405).end();

  const dateRe = /^\d{4}-\d{2}-\d{2}$/;
  const { date, from, to } = req.query;

  try {
    // ── Batch mode ────────────────────────────────────────────────────────
    if (from && to) {
      if (!dateRe.test(from) || !dateRe.test(to)) {
        return res.status(400).json({ ok: false, error: 'Parâmetros from/to devem ser YYYY-MM-DD' });
      }
      const data = await fetchRange(from, to);
      return res.status(200).json({ ok: true, data });
    }

    // ── Single date ───────────────────────────────────────────────────────
    if (!date || !dateRe.test(date)) {
      return res.status(400).json({ ok: false, error: 'Parâmetro date=YYYY-MM-DD ou from+to obrigatório' });
    }
    const result = await fetchSingle(date);
    return res.status(200).json({ ok: true, ...result });

  } catch (e) {
    console.error('[trafego-btag]', e.message);
    return res.status(500).json({ ok: false, error: e.message });
  }
};
