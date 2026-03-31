const crypto = require('crypto');
const Redis = require('ioredis');

const SALT = 'jogo2024';
const REDIS_KEY = 'jg-auth-users';
const INITIAL_USERS = [
  {
    email: 'harrisonwash@gmail.com',
    name: 'Harrison',
    role: 'admin',
    hash: 'b3afe5cde3c9f37be2fdae37f8ddcf11cd7464bda3cdd73d0d2c7645fd3b4551'
  }
];

function sha256(email, password) {
  return crypto.createHash('sha256').update(`${email}:${password}:${SALT}`).digest('hex');
}

let _redis = null;
function getRedis() {
  if (!_redis) {
    _redis = new Redis(process.env.REDIS_URL, {
      maxRetriesPerRequest: 1,
      connectTimeout: 3000,
      commandTimeout: 3000,
      lazyConnect: true,
      retryStrategy(times) {
        if (times > 1) return null; // give up after 1 retry
        return 500;
      }
    });
    _redis.on('error', (e) => console.error('Redis error:', e.message));
  }
  return _redis;
}

// Timeout wrapper — returns fallback if Redis takes too long
function withTimeout(promise, ms) {
  return Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), ms))
  ]);
}

async function getUsers() {
  let users = [...INITIAL_USERS];
  if (!process.env.REDIS_URL) return users;
  try {
    const redis = getRedis();
    await withTimeout(redis.connect().catch(() => {}), 2000);
    if (redis.status !== 'ready') return users;
    const data = await withTimeout(redis.get(REDIS_KEY), 2000);
    if (data) {
      const redisUsers = JSON.parse(data);
      const initialEmails = new Set(INITIAL_USERS.map(u => u.email.toLowerCase()));
      const extra = redisUsers.filter(u => !initialEmails.has(u.email.toLowerCase()));
      users = [...INITIAL_USERS, ...extra];
    }
    // Sync back to Redis (fire and forget)
    redis.set(REDIS_KEY, JSON.stringify(users)).catch(() => {});
  } catch (e) {
    console.error('getUsers fallback to INITIAL_USERS:', e.message);
  }
  return users;
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).end();

  try {
    const { email = '', password = '' } = req.body || {};
    const emailNorm = email.toLowerCase().trim();
    if (!emailNorm || !password) {
      return res.status(400).json({ error: 'E-mail e senha obrigatorios' });
    }

    const users = await getUsers();
    const h = sha256(emailNorm, password);
    const user = users.find(u => u.email.toLowerCase() === emailNorm && u.hash === h);
    if (!user) return res.status(401).json({ error: 'E-mail ou senha incorretos' });

    return res.status(200).json({ email: user.email, name: user.name, role: user.role });
  } catch (err) {
    console.error('login handler error:', err.message);
    return res.status(500).json({ error: 'Erro interno no servidor' });
  }
};
