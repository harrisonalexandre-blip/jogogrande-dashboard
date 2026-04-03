/**
 * api/trafego-ads.js
 * Modelo APPEND-ONLY: cada envio é um log com timestamp único.
 * Os totais diários são calculados no front somando todas as entradas do dia.
 *
 * Redis key: jg-trafego-logs-v3 → array de { id, d, ts, inv, imp, alc, clk, conv, freq, cad_meta, ftd_meta, ftdAmt_meta, depAmt_meta, netDep_meta, netPL_meta }
 * Sheets tab: "Tráfego Logs" (append permanente — nunca apaga)
 */

const Redis = require('ioredis');
const { google } = require('googleapis');

const REDIS_KEY  = 'jg-trafego-logs-v3';
const SHEET_ID   = '1ccyDzg1dGVwXkhZWN04eXVBrAobq0JvBNAH9Xp8fyhg';
const SHEET_TAB  = 'Tráfego Logs';

// ── Redis ──────────────────────────────────────────────────────────────────
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
async function redisSave(logs) {
  if (!process.env.REDIS_URL) return false;
  const redis = getRedis();
  await withTimeout(redis.connect().catch(() => {}), 2000);
  if (redis.status !== 'ready') return false;
  await withTimeout(redis.set(REDIS_KEY, JSON.stringify(logs)), 2000);
  return true;
}

// ── Google Sheets — append uma linha (nunca apaga) ────────────────────────
async function appendToSheets(entry) {
  if (!process.env.GOOGLE_CREDENTIALS) return { ok: false, reason: 'no credentials' };
  try {
    const creds = JSON.parse(process.env.GOOGLE_CREDENTIALS);
    const auth = new google.auth.GoogleAuth({
      credentials: creds,
      scopes: ['https://www.googleapis.com/auth/spreadsheets'],
    });
    const sheets = google.sheets({ version: 'v4', auth });

    // Garante que a aba existe
    const meta = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
    const tabExists = meta.data.sheets.some(s => s.properties.title === SHEET_TAB);
    if (!tabExists) {
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: SHEET_ID,
        requestBody: { requests: [{ addSheet: { properties: { title: SHEET_TAB } } }] }
      });
      // Cria cabeçalho na aba nova
      const header = ['ID','Data','Timestamp','Investimento','Impressões','Alcance','Clicks','Conv BM','Frequência','Cadastros Meta','FTDs Meta','FTD Amount Meta','Dep Amount Meta','Net Dep Meta','Net P&L Meta'];
      await sheets.spreadsheets.values.append({
        spreadsheetId: SHEET_ID,
        range: `${SHEET_TAB}!A1`,
        valueInputOption: 'USER_ENTERED',
        requestBody: { values: [header] }
      });
    }

    // Append apenas a nova linha
    const row = [
      entry.id, entry.d, entry.ts,
      entry.inv || 0, entry.imp || 0, entry.alc || 0,
      entry.clk || 0, entry.conv || 0, entry.freq || 0,
      entry.cad_meta || 0, entry.ftd_meta || 0, entry.ftdAmt_meta || 0,
      entry.depAmt_meta || 0, entry.netDep_meta || 0, entry.netPL_meta || 0
    ];
    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID,
      range: `${SHEET_TAB}!A:O`,
      valueInputOption: 'USER_ENTERED',
      requestBody: { values: [row] }
    });
    return { ok: true };
  } catch (e) {
    console.error('Sheets append error:', e.message);
    return { ok: false, reason: e.message };
  }
}

// ── Handler ────────────────────────────────────────────────────────────────
module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  // GET — retorna todos os logs
  if (req.method === 'GET') {
    try {
      const logs = await redisGet();
      return res.status(200).json({ ok: true, logs: logs.sort((a, b) => a.ts.localeCompare(b.ts)) });
    } catch (e) {
      return res.status(500).json({ error: e.message });
    }
  }

  // POST — adiciona entrada nova (APPEND, nunca substitui)
  if (req.method === 'POST') {
    try {
      const entry = req.body;
      if (!entry || !entry.d) return res.status(400).json({ error: 'Campo d (data) obrigatório' });

      // Garante ID único e timestamp
      entry.id  = entry.id  || (Date.now() + '-' + Math.random().toString(36).slice(2, 7));
      entry.ts  = entry.ts  || new Date().toISOString();

      const logs = await redisGet();
      logs.push(entry);

      await redisSave(logs);

      // Append assíncrono no Sheets (não bloqueia resposta)
      const sheetResult = await appendToSheets(entry).catch(e => ({ ok: false, reason: e.message }));

      return res.status(200).json({ ok: true, id: entry.id, count: logs.length, sheets: sheetResult });
    } catch (e) {
      return res.status(500).json({ error: e.message });
    }
  }

  // DELETE — remove entrada por ID
  if (req.method === 'DELETE') {
    try {
      const { id } = req.body || {};
      if (!id) return res.status(400).json({ error: 'Campo id obrigatório' });
      let logs = await redisGet();
      logs = logs.filter(l => l.id !== id);
      await redisSave(logs);
      return res.status(200).json({ ok: true, count: logs.length });
    } catch (e) {
      return res.status(500).json({ error: e.message });
    }
  }

  return res.status(405).end();
};
