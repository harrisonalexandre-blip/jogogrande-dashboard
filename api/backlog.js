const Redis = require('ioredis');

const REDIS_KEY = 'jg-backlog-items';

let _redis = null;
function getRedis() {
  if (!_redis) {
    _redis = new Redis(process.env.REDIS_URL, {
      maxRetriesPerRequest: 1,
      connectTimeout: 3000,
      commandTimeout: 3000,
      lazyConnect: true,
      retryStrategy(times) {
        if (times > 1) return null;
        return 500;
      }
    });
    _redis.on('error', (e) => console.error('Redis error:', e.message));
  }
  return _redis;
}

function withTimeout(promise, ms) {
  return Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), ms))
  ]);
}

async function getItems() {
  if (!process.env.REDIS_URL) return null;
  try {
    const redis = getRedis();
    await withTimeout(redis.connect().catch(() => {}), 2000);
    if (redis.status !== 'ready') return null;
    const data = await withTimeout(redis.get(REDIS_KEY), 2000);
    return data ? JSON.parse(data) : null;
  } catch (e) {
    console.error('backlog getItems error:', e.message);
    return null;
  }
}

async function saveItems(items) {
  if (!process.env.REDIS_URL) return false;
  try {
    const redis = getRedis();
    await withTimeout(redis.connect().catch(() => {}), 2000);
    if (redis.status !== 'ready') return false;
    await withTimeout(redis.set(REDIS_KEY, JSON.stringify(items)), 2000);
    return true;
  } catch (e) {
    console.error('backlog saveItems error:', e.message);
    return false;
  }
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') return res.status(200).end();

  // GET — retorna items do backlog
  if (req.method === 'GET') {
    try {
      const items = await getItems();
      return res.status(200).json({ items });
    } catch (err) {
      console.error('backlog GET error:', err.message);
      return res.status(500).json({ error: 'Erro ao carregar backlog' });
    }
  }

  // POST — salva items do backlog
  if (req.method === 'POST') {
    try {
      const { items } = req.body || {};
      if (!Array.isArray(items)) {
        return res.status(400).json({ error: 'items deve ser um array' });
      }
      const ok = await saveItems(items);
      if (!ok) return res.status(500).json({ error: 'Falha ao salvar no Redis' });
      return res.status(200).json({ ok: true, count: items.length });
    } catch (err) {
      console.error('backlog POST error:', err.message);
      return res.status(500).json({ error: 'Erro ao salvar backlog' });
    }
  }

  return res.status(405).end();
};
