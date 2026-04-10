const https = require('https');

const CH_HOST = 'analytics.phoenix365-prod.com';
const CH_PORT = 443;
const CH_USER = 'project_176_ro';
const CH_PASS = 'uChiThoocahtiek9';
const TZ = 'America/Sao_Paulo';

function chQuery(sql) {
  return new Promise((resolve, reject) => {
    const auth = Buffer.from(`${CH_USER}:${CH_PASS}`).toString('base64');
    const options = {
      hostname: CH_HOST,
      port: CH_PORT,
      path: '/?database=project_176',
      method: 'POST',
      headers: {
        'Authorization': `Basic ${auth}`,
        'Content-Type': 'text/plain; charset=utf-8',
        'Content-Length': Buffer.byteLength(sql, 'utf8')
      }
    };
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        if (res.statusCode === 200) resolve(data);
        else reject(new Error(`CH ${res.statusCode}: ${data.slice(0, 300)}`));
      });
    });
    req.on('error', reject);
    req.setTimeout(15000, () => { req.destroy(); reject(new Error('CH timeout')); });
    req.write(sql);
    req.end();
  });
}

// Today in BRT (UTC-3)
function todayBRT() {
  const now = new Date();
  const brt = new Date(now.getTime() - 3 * 60 * 60 * 1000);
  return brt.toISOString().split('T')[0];
}

module.exports = async function handler(req, res) {
  res.setHeader('Cache-Control', 'no-store, max-age=0');
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Content-Type', 'application/json');

  const today = todayBRT();

  try {
    // ─── Query 1: Daily KPIs (single-row scalar subqueries) ───
    const q1 = `SELECT
  (SELECT count() FROM project_176.client
   WHERE toDate(toTimezone(registration_time,'${TZ}'))='${today}' AND is_test=false) AS nr,
  (SELECT count() FROM project_176.deposits
   WHERE toDate(toTimezone(create_time,'${TZ}'))='${today}' AND status='PAID') AS dep_n,
  (SELECT sum(amount)/100 FROM project_176.deposits
   WHERE toDate(toTimezone(create_time,'${TZ}'))='${today}' AND status='PAID') AS dep_total,
  (SELECT count(DISTINCT d.client_id)
   FROM project_176.deposits d
   WHERE toDate(toTimezone(d.create_time,'${TZ}'))='${today}' AND d.status='PAID'
     AND d.client_id IN (
       SELECT client_id FROM project_176.deposits WHERE status='PAID'
       GROUP BY client_id HAVING min(toDate(toTimezone(create_time,'${TZ}')))='${today}'
     )) AS ftd,
  (SELECT sum(d.amount)/100
   FROM project_176.deposits d
   WHERE toDate(toTimezone(d.create_time,'${TZ}'))='${today}' AND d.status='PAID'
     AND d.client_id IN (
       SELECT client_id FROM project_176.deposits WHERE status='PAID'
       GROUP BY client_id HAVING min(toDate(toTimezone(create_time,'${TZ}')))='${today}'
     )) AS fda,
  (SELECT count() FROM project_176.withdrawals
   WHERE toDate(toTimezone(create_time,'${TZ}'))='${today}' AND status='PAID') AS wd_n,
  (SELECT sum(amount)/100 FROM project_176.withdrawals
   WHERE toDate(toTimezone(create_time,'${TZ}'))='${today}' AND status='PAID') AS wd_total,
  (SELECT sum(CASE WHEN status='BET' THEN amount ELSE 0 END)/100
   FROM project_176.casino
   WHERE toDate(toTimezone(spin_date,'${TZ}'))='${today}' AND is_free_spin=false) AS c_to,
  (SELECT (sum(CASE WHEN status='BET' THEN amount ELSE 0 END)-sum(CASE WHEN status='WIN' THEN amount ELSE 0 END))/100
   FROM project_176.casino
   WHERE toDate(toTimezone(spin_date,'${TZ}'))='${today}' AND is_free_spin=false) AS c_ggr,
  (SELECT sum(bet_amount)/100
   FROM project_176.bet_change
   WHERE toDate(toTimezone(create_time,'${TZ}'))='${today}' AND test_mode=false) AS sb_to,
  (SELECT (sum(bet_amount)-sum(CASE WHEN bet_status='WIN' THEN payout ELSE 0 END))/100
   FROM project_176.bet_change
   WHERE toDate(toTimezone(create_time,'${TZ}'))='${today}' AND test_mode=false) AS sb_ggr
FORMAT JSON`;

    // ─── Query 2: Hourly registrations ───
    const q2 = `SELECT toHour(toTimezone(registration_time,'${TZ}')) AS h, count() AS n
FROM project_176.client
WHERE toDate(toTimezone(registration_time,'${TZ}'))='${today}' AND is_test=false
GROUP BY h ORDER BY h FORMAT JSON`;

    // ─── Query 3: Hourly FTDs (assign each FTD to hour of FIRST deposit only) ───
    const q3 = `WITH ftd_clients AS (
    SELECT client_id FROM project_176.deposits WHERE status='PAID'
    GROUP BY client_id HAVING min(toDate(toTimezone(create_time,'${TZ}')))='${today}'
),
ftd_first_hour AS (
    SELECT d.client_id, toHour(toTimezone(min(d.create_time),'${TZ}')) AS h
    FROM project_176.deposits d
    JOIN ftd_clients fc ON d.client_id = fc.client_id
    WHERE toDate(toTimezone(d.create_time,'${TZ}'))='${today}' AND d.status='PAID'
    GROUP BY d.client_id
)
SELECT h, count() AS n FROM ftd_first_hour GROUP BY h ORDER BY h FORMAT JSON`;

    // ─── Query 4: Hourly casino volume ───
    const q4 = `SELECT toHour(toTimezone(spin_date,'${TZ}')) AS h,
  sum(CASE WHEN status='BET' THEN amount ELSE 0 END)/100 AS n
FROM project_176.casino
WHERE toDate(toTimezone(spin_date,'${TZ}'))='${today}' AND is_free_spin=false
GROUP BY h ORDER BY h FORMAT JSON`;

    // Run all queries in parallel
    const [r1, r2, r3, r4] = await Promise.all([
      chQuery(q1), chQuery(q2), chQuery(q3), chQuery(q4)
    ]);

    const d1   = JSON.parse(r1).data[0] || {};
    const regsRows = JSON.parse(r2).data || [];
    const ftdRows  = JSON.parse(r3).data || [];
    const volRows  = JSON.parse(r4).data || [];

    // Build 24h arrays
    const hrRegs = Array(24).fill(0);
    const hrFtd  = Array(24).fill(0);
    const hrVol  = Array(24).fill(0);
    regsRows.forEach(r => { hrRegs[parseInt(r.h)] = parseInt(r.n); });
    ftdRows.forEach(r  => { hrFtd[parseInt(r.h)]  = parseInt(r.n); });
    volRows.forEach(r  => { hrVol[parseInt(r.h)]  = Math.round(parseFloat(r.n) * 100) / 100; });

    const f  = v => Math.round((parseFloat(v) || 0) * 100) / 100;
    const fi = v => parseInt(v) || 0;

    const dep_total = f(d1.dep_total);
    const wd_total  = f(d1.wd_total);
    const c_ggr     = f(d1.c_ggr);
    const sb_ggr    = f(d1.sb_ggr);
    const c_to      = f(d1.c_to);
    const sb_to     = f(d1.sb_to);

    const payload = {
      date:      today,
      ts:        new Date().toISOString(),
      nr:        fi(d1.nr),
      dep_n:     fi(d1.dep_n),
      dep_total: dep_total,
      ftd:       fi(d1.ftd),
      fda:       f(d1.fda),
      wd_n:      fi(d1.wd_n),
      wd_total:  wd_total,
      c_to:      c_to,
      c_ggr:     c_ggr,
      sb_to:     sb_to,
      sb_ggr:    sb_ggr,
      ngr:       Math.round((c_ggr + sb_ggr) * 100) / 100,
      nc:        Math.round((dep_total - wd_total) * 100) / 100,
      hourly:    {
        regs: hrRegs,
        ftd:  hrFtd,
        vol:  hrVol
      }
    };

    res.status(200).json(payload);

  } catch (e) {
    console.error('live.js error:', e.message);
    res.status(500).json({ error: e.message, date: today });
  }
};
