/**
 * api/trafego-btag.js
 * Consulta ClickHouse direto para dados do BTAG "Trafego Direto - Meta ads" (aff_id=524865)
 * GET /api/trafego-btag?date=YYYY-MM-DD
 * Retorna: { ok, date, cadastros, ftds, ftdAmt, depAmt, wdAmt, netDep, netPL }
 */

const https = require('https');

const CH_HOST = 'analytics.phoenix365-prod.com';
const CH_USER = 'project_176_ro';
const CH_PASS = 'uChiThoocahtiek9';

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
        try {
          resolve(JSON.parse(data));
        } catch (e) {
          reject(new Error('Parse error: ' + data.slice(0, 200)));
        }
      });
    });
    req.on('error', reject);
    req.setTimeout(15000, () => { req.destroy(); reject(new Error('timeout')); });
    req.end();
  });
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'GET') return res.status(405).end();

  const { date } = req.query;
  if (!date || !/^\d{4}-\d{2}-\d{2}$/.test(date)) {
    return res.status(400).json({ ok: false, error: 'Parâmetro date=YYYY-MM-DD obrigatório' });
  }

  try {
    // ── Cadastros (registros com BTAG aff_id=524865) ─────────────────────
    const sqlCad = `
      SELECT count() as cadastros
      FROM project_176.client
      WHERE position(btag, 'aff_id=524865') > 0
        AND is_test = false
        AND toDate(toTimezone(registration_time, 'America/Sao_Paulo')) = '${date}'
    `;

    // ── FTDs: clientes com BTAG cujo primeiro depósito foi nesse dia ──────
    const sqlFtd = `
      SELECT
        count() as ftds,
        sum(d.amount) / 100 as ftdAmt
      FROM project_176.deposits d
      INNER JOIN (
        SELECT id, btag
        FROM project_176.client
        WHERE position(btag, 'aff_id=524865') > 0
          AND is_test = false
      ) c ON c.id = d.client_id
      INNER JOIN (
        SELECT client_id, min(toDate(toTimezone(created_at, 'America/Sao_Paulo'))) as first_dep_date
        FROM project_176.deposits
        WHERE status = 'PAID'
        GROUP BY client_id
      ) fd ON fd.client_id = d.client_id
      WHERE d.status = 'PAID'
        AND fd.first_dep_date = '${date}'
        AND toDate(toTimezone(d.created_at, 'America/Sao_Paulo')) = '${date}'
    `;

    // ── Depósitos totais do dia (clientes com BTAG) ───────────────────────
    const sqlDep = `
      SELECT
        count() as dep_count,
        sum(d.amount) / 100 as depAmt
      FROM project_176.deposits d
      INNER JOIN (
        SELECT id
        FROM project_176.client
        WHERE position(btag, 'aff_id=524865') > 0
          AND is_test = false
      ) c ON c.id = d.client_id
      WHERE d.status = 'PAID'
        AND toDate(toTimezone(d.created_at, 'America/Sao_Paulo')) = '${date}'
    `;

    // ── Saques totais do dia (clientes com BTAG) ──────────────────────────
    const sqlWd = `
      SELECT
        count() as wd_count,
        sum(w.amount) / 100 as wdAmt
      FROM project_176.withdrawals w
      INNER JOIN (
        SELECT id
        FROM project_176.client
        WHERE position(btag, 'aff_id=524865') > 0
          AND is_test = false
      ) c ON c.id = w.client_id
      WHERE w.status = 'PAID'
        AND toDate(toTimezone(w.created_at, 'America/Sao_Paulo')) = '${date}'
    `;

    // Execute all queries in parallel
    const [rCad, rFtd, rDep, rWd] = await Promise.all([
      queryClickHouse(sqlCad),
      queryClickHouse(sqlFtd),
      queryClickHouse(sqlDep),
      queryClickHouse(sqlWd),
    ]);

    const cadastros = parseInt(rCad.data?.[0]?.cadastros || 0);
    const ftds      = parseInt(rFtd.data?.[0]?.ftds || 0);
    const ftdAmt    = parseFloat(rFtd.data?.[0]?.ftdAmt || 0);
    const depAmt    = parseFloat(rDep.data?.[0]?.depAmt || 0);
    const wdAmt     = parseFloat(rWd.data?.[0]?.wdAmt || 0);
    const netDep    = depAmt - wdAmt;
    // netPL = depAmt - wdAmt (sem dados de casino nesse endpoint por performance)

    return res.status(200).json({
      ok: true,
      date,
      cadastros,
      ftds,
      ftdAmt: Math.round(ftdAmt * 100) / 100,
      depAmt: Math.round(depAmt * 100) / 100,
      wdAmt:  Math.round(wdAmt  * 100) / 100,
      netDep: Math.round(netDep * 100) / 100,
      netPL:  Math.round(netDep * 100) / 100, // proxy sem casino
    });

  } catch (e) {
    console.error('[trafego-btag]', e.message);
    return res.status(500).json({ ok: false, error: e.message });
  }
};
