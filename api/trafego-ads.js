const Redis = require('ioredis');
const { google } = require('googleapis');

const REDIS_KEY = 'jg-trafego-ads-v2';
const SHEET_ID = '1ccyDzg1dGVwXkhZWN04eXVBrAobq0JvBNAH9Xp8fyhg';
const SHEET_TAB = 'Tráfego Pago';

// ── Redis ──────────────────────────────────────────────────────────────
let _redis = null;
function getRedis() {
  if (!_redis) {
    _redis = new Redis(process.env.REDIS_URL, {
      maxRetriesPerRequest: 1, connectTimeout: 3000, commandTimeout: 3000,
      lazyConnect: true,
      retryStrategy(t) { return t > 1 ? null : 500; }
    });
    _redis.on('error', e => console.error('Redis:', e.message));
  }
  return _redis;
}
function withTimeout(p, ms) {
  return Promise.race([p, new Promise((_, r) => setTimeout(() => r(new Error('timeout')), ms))]);
}
async function redisGet() {
  if (!process.env.REDIS_URL) return [];
  const redis = getRedis();
  await withTimeout(redis.connect().catch(() => {}), 2000);
  if (redis.status !== 'ready') return [];
  const raw = await withTimeout(redis.get(REDIS_KEY), 2000);
  return raw ? JSON.parse(raw) : [];
}
async function redisSave(items) {
  if (!process.env.REDIS_URL) return false;
  const redis = getRedis();
  await withTimeout(redis.connect().catch(() => {}), 2000);
  if (redis.status !== 'ready') return false;
  await withTimeout(redis.set(REDIS_KEY, JSON.stringify(items)), 2000);
  return true;
}

// ── Google Sheets Sync ────────────────────────────────────────────────
async function syncToSheets(items) {
  if (!process.env.GOOGLE_CREDENTIALS) return { ok: false, reason: 'no credentials' };
  try {
    const creds = JSON.parse(process.env.GOOGLE_CREDENTIALS);
    const auth = new google.auth.GoogleAuth({
      credentials: creds,
      scopes: ['https://www.googleapis.com/auth/spreadsheets'],
    });
    const sheets = google.sheets({ version: 'v4', auth });

    // Ensure tab exists
    const meta = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
    const tabExists = meta.data.sheets.some(s => s.properties.title === SHEET_TAB);
    if (!tabExists) {
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: SHEET_ID,
        requestBody: { requests: [{ addSheet: { properties: { title: SHEET_TAB } } }] }
      });
    }

    // Build rows
    const header = ['Data','Investimento','Impressões','Alcance','CPM','CTR%','CPC','Clicks','Conv BM','Frequência','CPR','Cadastros','FTDs','FTD Amount','Dep Amount','Net Deposit','Net P&L','Custo CAD','Custo FTD','ROAS','Atualizado'];
    const rows = items.sort((a,b) => a.d.localeCompare(b.d)).map(r => [
      r.d, r.inv||0, r.imp||0, r.alc||0, r.cpm||0, r.ctr||0, r.cpc||0,
      r.clk||0, r.conv||0, r.freq||0, r.cpr||0,
      r.cad||0, r.ftd||0, r.ftdAmt||0, r.depAmt||0, r.netDep||0, r.netPL||0,
      r.cad>0?(r.inv/r.cad).toFixed(2):0,
      r.ftd>0?(r.inv/r.ftd).toFixed(2):0,
      r.inv>0?(r.netPL/r.inv).toFixed(3):0,
      r.savedAt||''
    ]);

    // Clear and rewrite
    await sheets.spreadsheets.values.clear({
      spreadsheetId: SHEET_ID,
      range: `${SHEET_TAB}!A:U`
    });
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID,
      range: `${SHEET_TAB}!A1`,
      valueInputOption: 'USER_ENTERED',
      requestBody: { values: [header, ...rows] }
    });
    return { ok: true, rows: rows.length };
  } catch (e) {
    console.error('Sheets sync error:', e.message);
    return { ok: false, reason: e.message };
  }
}

// ── Handler ───────────────────────────────────────────────────────────
module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  // GET — retorna todos os registros
  if (req.method === 'GET') {
    try {
      const items = await redisGet();
      return res.status(200).json({ ok: true, items });
    } catch (e) {
      return res.status(500).json({ error: e.message });
    }
  }

  // POST — salva/atualiza um dia + sync Sheets
  if (req.method === 'POST') {
    try {
      const entry = req.body;
      if (!entry || !entry.d) return res.status(400).json({ error: 'Campo d (data) obrigatório' });
      entry.savedAt = new Date().toISOString();

      let items = await redisGet();
      const idx = items.findIndex(i => i.d === entry.d);
      if (idx >= 0) items[idx] = { ...items[idx], ...entry }; // merge (preserva campos existentes)
      else items.push(entry);
      items.sort((a, b) => a.d.localeCompare(b.d));

      await redisSave(items);

      // Sync to Google Sheets (async, não bloqueia resposta)
      const sheetResult = await syncToSheets(items).catch(e => ({ ok: false, reason: e.message }));

      return res.status(200).json({ ok: true, count: items.length, sheets: sheetResult });
    } catch (e) {
      return res.status(500).json({ error: e.message });
    }
  }

  // DELETE — remove um dia
  if (req.method === 'DELETE') {
    try {
      const { d } = req.body || {};
      if (!d) return res.status(400).json({ error: 'Campo d obrigatório' });
      let items = await redisGet();
      items = items.filter(i => i.d !== d);
      await redisSave(items);
      return res.status(200).json({ ok: true, count: items.length });
    } catch (e) {
      return res.status(500).json({ error: e.message });
    }
  }

  return res.status(405).end();
};
